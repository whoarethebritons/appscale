import logging
import os
import socket
from datetime import timedelta

import yaml
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.httpclient import HTTPError
from tornado.ioloop import IOLoop
from tornado.locks import Lock as AsyncLock

from appscale.common import appscale_info
from appscale.common import constants
from appscale.common import monit_app_configuration
from appscale.common.constants import MonitStates
from appscale.common.deployment_config import ConfigInaccessible
from . import deployment_config
from . import utils

# The number of seconds to wait for an application to start up.
START_APP_TIMEOUT = 180

# The first port that can be used for starting revision instances.
STARTING_APPENGINE_PORT = 20000

PIDFILE_TEMPLATE = os.path.join('/', 'var', 'run', 'appscale',
                                'app___{revision_key}-{port}.pid')

# The location on the filesystem where the PHP executable is installed.
PHP_CGI_LOCATION = os.path.join('/', 'usr', 'bin', 'php-cgi')

# The location of the App Engine SDK for Go.
GO_SDK = os.path.join('/', 'opt', 'go_appengine')

# Projects that can access any application's data.
TRUSTED_PROJECTS = ['appscaledashboard']

# asdf
port_assignment_lock = AsyncLock()

# A global dictionary that keeps track of asdf.
current_state = {}


class RevisionInstance(object):
  FAILED = 'failed'
  RUNNING = 'running'
  STARTING = 'starting'
  STOPPING = 'stopping'
  UNKNOWN = 'unknown'  # Instance was discovered from Monit.

  HEALTH_CHECK = '_ah/health_check'

  def __init__(self, revision_key):
    self.revision_key = revision_key
    self.state = self.UNKNOWN
    self.monit_state = MonitStates.MISSING

    self.port = None
    self.source_location = None
    self.runtime = None
    self.max_memory = None
    self.future = None

  def start(self, source_manager, thread_pool, monit_operator, zk_client):
    required_fields = [self.source_location, self.runtime, self.max_memory]
    for field in required_fields:
      assert field is not None, '{} must be set'.format(field)

    io_loop = IOLoop.current()
    self.state = self.STARTING
    future = self._start(source_manager, thread_pool, monit_operator,
                         zk_client)
    self.future = gen.with_timeout(
      timedelta(seconds=START_APP_TIMEOUT), future, io_loop)
    io_loop.add_future(self.future, self._handle_start_result)

  def potentially_running(self):
    if self.state == self.RUNNING and self.monit_state == MonitStates.RUNNING:
      return True

    if self.state == self.STARTING:
      return True

    stopped_monit_states = [MonitStates.UNMONITORED, MonitStates.MISSING]
    if (self.state == self.UNKNOWN and
        self.monit_state not in stopped_monit_states):
      return True

    return False

  @gen.coroutine
  def _find_lowest_free_port(self, thread_pool):
    with (yield port_assignment_lock.acquire()):
      assigned_ports = set()
      for revision_id in current_state:
        for instance in current_state[revision_id]['instances']:
          assigned_ports.add(instance.port)

      port = STARTING_APPENGINE_PORT
      while True:
        # Skip ports that have been assigned.
        if port in assigned_ports:
          port += 1
          continue

        # Skip ports that are in use.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
          yield thread_pool.submit(sock.bind, ('127.0.0.1', port))
        except socket.error:
          port += 1
          continue
        finally:
          sock.close()

        raise gen.Return(port)

  def _java_start_cmd(self, project_id, pidfile):
    """ Creates the start command to run the java application server.

    Args:
      project_id: A string specifying the project ID.
      pidfile: A string specifying the pidfile location.
    Returns:
      A string specifying the start command.
    """
    project_id, service_id, version_id, _ = self.revision_key.split('_')
    source_path = os.path.join(constants.UNPACK_ROOT, self.revision_key, 'app')
    web_inf = utils.find_web_inf(source_path)
    db_proxy = appscale_info.get_db_proxy()
    tq_proxy = appscale_info.get_tq_proxy()
    load_balancer_host = appscale_info.get_login_ip()
    java_start_script = os.path.join(
      constants.JAVA_APPSERVER, 'appengine-java-sdk-repacked', 'bin',
      'dev_appserver.sh')

    # Account for MaxPermSize (~170MB), the parent process (~50MB), and
    # thread stacks (~20MB).
    max_heap = self.max_memory - 250
    assert max_heap > 0, 'Insufficient memory for revision'

    cmd = [
      java_start_script,
      "--port=" + str(self.port),
      # Allow javax.email to connect to the smtp server.
      "--jvm_flag=-Dsocket.permit_connect=true",
      '--jvm_flag=-Xmx{}m'.format(max_heap),
      # A slow /dev/random can delay starting the process.
      '--jvm_flag=-Djava.security.egd=file:/dev/./urandom',
      "--disable_update_check",
      "--address=" + appscale_info.get_private_ip(),
      "--datastore_path=" + db_proxy,
      "--login_server=" + load_balancer_host,
      "--appscale_version=1",
      "--APP_NAME=" + project_id,
      "--SERVICE_ID=" + service_id,
      "--VERSION_ID=" + version_id,
      "--NGINX_ADDRESS=" + load_balancer_host,
      "--TQ_PROXY=" + tq_proxy,
      "--pidfile={}".format(pidfile),
      os.path.dirname(web_inf)
    ]

    return ' '.join(cmd)

  def _python27_start_cmd(self, project_id, pidfile):
    """ Creates the start command to run the python application server.

    Args:
      app_name: The name of the application to run
      login_ip: The public IP of this deployment
      port: The local port the application server will bind to
      pidfile: A string specifying the pidfile location.
    Returns:
      A string of the start command.
    """
    source_path = os.path.join(constants.UNPACK_ROOT, self.revision_key, 'app')
    dev_appserver = os.path.join(constants.APPSCALE_HOME, 'AppServer',
                                 'dev_appserver.py')
    load_balancer_host = appscale_info.get_login_ip()
    db_proxy = appscale_info.get_db_proxy()
    private_ip = appscale_info.get_private_ip()

    flags = [
      '--port', str(self.port),
      '--admin_port', str(self.port + 10000),
      '--login_server', load_balancer_host,
      '--skip_sdk_update_check',
      '--nginx_host', load_balancer_host,
      '--require_indexes',
      '--enable_sendmail',
      '--xmpp_path', load_balancer_host,
      '--php_executable_path', PHP_CGI_LOCATION,
      '--uaserver_path', '{}:{}'.format(db_proxy, constants.UA_SERVER_PORT),
      '--datastore_path', '{}:{}'.format(db_proxy, constants.DB_SERVER_PORT),
      '--host', private_ip,
      '--admin_host', private_ip,
      '--automatic_restart', 'no',
      '--pidfile', pidfile
    ]
    if project_id in TRUSTED_PROJECTS:
      flags.append('--trusted')

    cmd = ['/usr/bin/python2', dev_appserver] + [source_path] + flags
    return ' '.join(cmd)

  def _get_env_vars(self):
    source_path = os.path.join(constants.UNPACK_ROOT, self.revision_key, 'app')
    project_id = self.revision_key.split('_')[0]

    if self.runtime == constants.JAVA:
      web_inf = utils.find_web_inf(source_path)
      xml_location = os.path.join(web_inf, 'appengine-web.xml')
      env_vars = utils.extract_env_vars_from_xml(xml_location)

      gcs_config = {'scheme': 'https', 'port': 443}
      try:
        gcs_config.update(deployment_config.get_config('gcs'))
      except ConfigInaccessible:
        logging.warning('Unable to fetch GCS configuration.')

      if 'host' in gcs_config:
        env_vars['GCS_HOST'] = '{scheme}://{host}:{port}'.format(**gcs_config)
    else:
      login_ip = appscale_info.get_login_ip()
      yaml_location = os.path.join(source_path, 'app.yaml')
      with open(yaml_location) as yaml_file:
        app_config = yaml.safe_load(yaml_file)

      env_vars = app_config.get('env_variables', {})
      env_vars['MY_IP_ADDRESS'] = login_ip
      env_vars['APPNAME'] = project_id
      env_vars['GOMAXPROCS'] = appscale_info.get_num_cpus()
      env_vars['PYTHON_LIB'] = os.path.join(constants.APPSCALE_HOME,
                                            'AppServer')

    env_vars['APPSCALE_HOME'] = constants.APPSCALE_HOME
    return env_vars

  def _write_monit_config(self):
    project_id = self.revision_key.split('_')[0]
    revision_path = os.path.join(constants.UNPACK_ROOT, self.revision_key)
    env_vars = self._get_env_vars()
    pidfile = PIDFILE_TEMPLATE.format(
      revision_key=self.revision_key, port=self.port)
    syslog_server = appscale_info.get_headnode_ip()

    if self.runtime == constants.GO:
      env_vars['GOPATH'] = os.path.join(revision_path, 'gopath')
      env_vars['GOROOT'] = os.path.join(GO_SDK, 'goroot')

    watch = 'app___{}'.format(self.revision_key)

    if self.runtime == constants.JAVA:
      start_cmd = self._java_start_cmd(project_id, pidfile)
    else:
      start_cmd = self._python27_start_cmd(project_id, pidfile)

    monit_app_configuration.create_config_file(
      watch,
      start_cmd,
      pidfile,
      self.port,
      env_vars,
      self.max_memory,
      syslog_server,
      check_port=True)

    return '-'.join([watch, str(self.port)])

  @gen.coroutine
  def _start(self, source_manager, thread_pool, monit_operator, zk_client):
    yield source_manager.ensure_source(
      self.revision_key, self.source_location, self.runtime)
    self.port = yield self._find_lowest_free_port(thread_pool)
    monit_entry = self._write_monit_config()
    logging.info('Starting {} on {}'.format(self.revision_key, self.port))
    yield monit_operator.reload()
    yield monit_operator.ensure_running(monit_entry)

    private_ip = appscale_info.get_private_ip()
    revision_url = 'http://{}:{}/{}'.format(private_ip, self.port,
                                            self.HEALTH_CHECK)
    client = AsyncHTTPClient()
    while True:
      try:
        yield client.fetch(revision_url)
        break
      except socket.error:
        yield gen.sleep(1)
        continue
      except HTTPError:
        # If the server returns any HTTP response, it's serving.
        break

    logging.info('{}:{} is ready'.format(self.revision_key, self.port))

    # Indicate instance availability.
    instances_node = '/appscale/instances/{}/{}:{}'.format(
      self.revision_key, private_ip, self.port)
    yield thread_pool.submit(zk_client.ensure_path, instances_node)

  def _handle_start_result(self, future):
    try:
      future.result()
      self.state = self.RUNNING
      self.future = None
    except Exception:
      self.state = self.FAILED
      logging.exception(
        '{}:{} failed to start'.format(self.revision_key, self.port))

  def __repr__(self):
    return '<RevisionInstance: {}, {}>'.format(self.revision_key, self.state)
