""" This service starts and stops application servers of a given application. """


import glob
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from xml.etree import ElementTree

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.locks import Lock as AsyncLock
from tornado.httpclient import AsyncHTTPClient
from kazoo.client import KazooClient

from appscale.common import (
  appscale_info,
  constants,
  file_io,
  monit_interface,
  misc
)
from appscale.admin.revision_manager.revision_instance import RevisionInstance
from appscale.admin.revision_manager.revision_instance import current_state
from appscale.admin.revision_manager.source_manager import SourceManager
from appscale.common.monit_app_configuration import MONIT_CONFIG_DIR
from appscale.common.version_manager import VersionManager
from appscale.common.monit_interface import MonitOperator

from appscale.admin.revision_manager import deployment_config

# The amount of seconds to wait for an application to start up.
START_APP_TIMEOUT = 180

# The amount of seconds to wait between checking if an application is up.
BACKOFF_TIME = 1

# The PID number to return when a process did not start correctly
BAD_PID = -1

# Default hourly cron directory.
CRON_HOURLY = '/etc/cron.hourly'



# Max log size for AppScale Dashboard servers.
DASHBOARD_LOG_SIZE = 10 * 1024 * 1024

# Max application server log size in bytes.
APP_LOG_SIZE = 250 * 1024 * 1024

# Default logrotate configuration directory.
LOGROTATE_CONFIG_DIR = '/etc/logrotate.d'

# Required configuration fields for starting an application
REQUIRED_CONFIG_FIELDS = [
  'app_name',
  'app_port',
  'language',
  'login_ip',
  'env_vars',
  'max_memory']

# The web path to fetch to see if the application is up
FETCH_PATH = '/_ah/health_check'

# The app ID of the AppScale Dashboard.
APPSCALE_DASHBOARD_ID = "appscaledashboard"



# The flag to tell the application server that this application can access
# all application data.
TRUSTED_FLAG = "--trusted"





HTTP_OK = 200

# The amount of seconds to wait before retrying to add routing.
ROUTING_RETRY_INTERVAL = 5

PIDFILE_TEMPLATE = os.path.join('/', 'var', 'run', 'appscale',
                                'app___{project}-{port}.pid')

MONIT_APP_PREFIX = 'app___'



source_manager = None

desired_state = {}

archives = {}

monit_lock = AsyncLock()

# Prevent stuff.
assignment_lock = AsyncLock()

fetched_assignments = False

version_manager = None

monit_operator = None


class Runtimes(object):
  PYTHON = 'python27'
  GO = 'go'
  JAVA = 'java'
  PHP = 'php'
  UNKNOWN = 'unknown'





def stop_app_instance(app_name, port):
  """ Stops a Google App Engine application process instance on current
      machine.

  Args:
    app_name: A string, the name of application to stop.
    port: The port the application is running on.
  Returns:
    True on success, False otherwise.
  """
  if not misc.is_app_name_valid(app_name):
    logging.error("Unable to kill app process %s on port %d because of " \
      "invalid name for application" % (app_name, int(port)))
    return False

  logging.info("Stopping application %s" % app_name)
  watch = "app___" + app_name + "-" + str(port)
  if not monit_interface.stop(watch, is_group=False):
    logging.error("Unable to stop application server for app {0} on " \
      "port {1}".format(app_name, port))
    return False

  # Now that the AppServer is stopped, remove its monit config file so that
  # monit doesn't pick it up and restart it.
  monit_config_file = '{}/appscale-{}.cfg'.format(MONIT_CONFIG_DIR, watch)
  try:
    os.remove(monit_config_file)
  except OSError as os_error:
    logging.error("Error deleting {0}".format(monit_config_file))

  return True


def stop_app(app_name):
  """ Stops all process instances of a Google App Engine application on this
      machine.

  Args:
    app_name: Name of application to stop
  Returns:
    True on success, False otherwise
  """
  if not misc.is_app_name_valid(app_name):
    logging.error("Unable to kill app process %s on because of " \
      "invalid name for application" % (app_name))
    return False

  logging.info("Stopping application %s" % app_name)
  watch = "app___" + app_name
  monit_result = monit_interface.stop(watch)

  if not monit_result:
    logging.error("Unable to shut down monit interface for watch %s" % watch)
    return False

  # Remove the monit config files for the application.
  # TODO: Reload monit to pick up config changes.
  config_files = glob.glob('{}/appscale-{}-*.cfg'.format(MONIT_CONFIG_DIR, watch))
  for config_file in config_files:
    try:
      os.remove(config_file)
    except OSError:
      logging.exception('Error removing {}'.format(config_file))

  if not remove_logrotate(app_name):
    logging.error("Error while setting up log rotation for application: {}".
      format(app_name))

  return True


def remove_logrotate(app_name):
  """ Removes logrotate script for the given application.

  Args:
    app_name: A string, the name of the application to remove logrotate for.
  Returns:
    True on success, False otherwise.
  """
  app_logrotate_script = "{0}/appscale-{1}".\
    format(LOGROTATE_CONFIG_DIR, app_name)
  logging.debug("Removing script: {}".format(app_logrotate_script))

  try:
    os.remove(app_logrotate_script)
  except OSError:
    logging.error("Error deleting {0}".format(app_logrotate_script))
    return False

  return True














def create_python27_stop_cmd(port):
  """ This creates the stop command for an application which is
  uniquely identified by a port number. Additional portions of the
  start command are included to prevent the termination of other
  processes.

  Args:
    port: The port which the application server is running
  Returns:
    A string of the stop command.
  """
  stop_cmd = "/usr/bin/python2 {0}/scripts/stop_service.py " \
    "dev_appserver.py {1}".format(constants.APPSCALE_HOME, port)
  return stop_cmd





def is_config_valid(config):
  """ Takes a configuration and checks to make sure all required properties
    are present.

  Args:
    config: The dictionary to validate
  Returns:
    True if valid, False otherwise
  """
  for ii in REQUIRED_CONFIG_FIELDS:
    try:
      if config[ii]:
        pass
    except KeyError:
      logging.error("Unable to find " + str(ii) + " in configuration")
      return False
  return True


@gen.coroutine
def monit_revision_entries():
  client = AsyncHTTPClient()
  url = 'http://localhost:2812/_status?format=xml'
  response = yield client.fetch(url)
  root = ElementTree.XML(response.body)
  entries = []
  for service in root.iter('service'):
    name = service.find('name').text
    monitored = int(service.find('monitor').text)
    status = int(service.find('status').text)
    if not name.startswith(MONIT_APP_PREFIX):
      continue

    revision, port = name[len(MONIT_APP_PREFIX):].rsplit('-', 1)
    if monitored == 0:
      state = constants.MonitStates.UNMONITORED
    elif monitored == 1:
      if status == 0:
        state = constants.MonitStates.RUNNING
      else:
        state = constants.MonitStates.STOPPED
    else:
      state = constants.MonitStates.PENDING
    entries.append({'revision': revision, 'port': port, 'state': state})

  raise gen.Return(entries)


def start_instances(revision_key, count):
  revision = desired_state[revision_key]
  new_instances = []
  for _ in range(count):
    instance = RevisionInstance(revision_key)
    instance.source_location = revision['sourceArchive']
    instance.runtime = revision['runtime']
    instance.max_memory = revision['maxMemory']
    instance.start(source_manager, thread_pool, monit_operator, zk_client)
    new_instances.append(instance)
  return new_instances


def setup_logrotate(revision_key, log_size):
  """ Creates a logrotate script for the logs that the given application
      will create.

  Args:
    app_name: A string, the application ID.
    watch: A string of the form 'app___<app_ID>'.
    log_size: An integer, the size of logs that are kept per application server.
      The size should be in bytes.
  Returns:
    True on success, False otherwise.
  """
  # Write application specific logrotation script.
  app_logrotate_script = os.path.join(
    LOGROTATE_CONFIG_DIR, 'appscale-{}'.format(revision_key))

  watch = 'app___{}'.format(revision_key)

  # Application logrotate script content.
  contents = """/var/log/appscale/{watch}*.log {{
  size {size}
  missingok
  rotate 7
  compress
  delaycompress
  notifempty
  copytruncate
}}
""".format(watch=watch, size=log_size)

  with open(app_logrotate_script, 'w') as app_logrotate_fd:
    app_logrotate_fd.write(contents)


@gen.coroutine
def reconcile():
  # Update current state with Monit info.
  monit_entries = yield monit_revision_entries()
  for revision_key in current_state:
    for instance in current_state[revision_key]['instances']:
      instance.monit_state = constants.MonitStates.MISSING

  for entry in monit_entries:
    revision_key = entry['revision']
    port = entry['port']
    if revision_key not in current_state:
      current_state[revision_key] = {
        'instances': [],
        'runtime': Runtimes.UNKNOWN,
        'archive': SourceManager.UNKNOWN_LOCATION}

    revision = current_state[revision_key]
    try:
      instance = next(instance for instance in revision['instances']
                      if instance.port == port)
      instance.monit_state = entry['state']
    except StopIteration:
      instance = RevisionInstance(revision_key)
      instance.port = port
      instance.monit_state = entry['state']
      revision['instances'].append(instance)

  # Don't alter current state if assignment is unknown.
  if not fetched_assignments:
    raise gen.Return()

  logging.info('current_state: {}'.format(current_state))
  for revision_key in current_state:
    for instance in current_state[revision_key]['instances']:
      if instance.state == RevisionInstance.FAILED:
        logging.info(instance.future.exception())

  # Update current state with assignment.
  for revision_key in desired_state:
    desired_revision = desired_state[revision_key]
    if revision_key not in current_state:
      logging.info('New revision: {}'.format(revision_key))
      current_state[revision_key] = {
        'instances': [],
        'runtime': desired_revision['runtime'],
        'archive': desired_revision['sourceArchive']}
      setup_logrotate(revision_key, desired_revision['logSize'])

    current_revision = current_state[revision_key]
    current_revision['runtime'] = desired_revision['runtime']
    current_revision['archive'] = desired_revision['sourceArchive']
    potential = [instance for instance in current_revision['instances']
                 if instance.potentially_running()]
    to_start = desired_revision['instanceCount'] - len(potential)
    new_instances = start_instances(revision_key, to_start)
    current_revision['instances'].extend(new_instances)


@gen.coroutine
def update_assignment(new_layout):
  """ Updates the desired state.

  Args:
    new_layout: A dictionary specifying an assignment of revisions.
  """
  logging.info('Assignment: {}'.format(new_layout))

  global desired_state, fetched_assignments
  desired_state = new_layout
  fetched_assignments = True
  logging.info('updating versions')
  version_manager.update_versions(desired_state.keys())
  with (yield assignment_lock.acquire()):
    logging.info('reconciling')
    reconcile()


def assignment_watcher(data, _):
  """ Passes the given data to the main IOLoop.

  This is designed to be used as a ZooKeeper watch target function. Because of
  this, it runs in a separate thread. In order to avoid having to use thread
  locks, it passes the assignment data to the main thread.

  Args:
    data: A string containing ZooKeeper node data.
  """
  if data is None:
    new_layout = {}
  else:
    new_layout = json.loads(data)

  # Bridge back to main thread's IOLoop.
  main_io_loop = IOLoop.instance()
  main_io_loop.add_callback(update_assignment, new_layout)


@gen.coroutine
def watchdog_loop():
  """ Makes sure the assignment of revisions is followed. """
  while True:
    with (yield assignment_lock.acquire()):
      try:
        yield reconcile()
      except Exception:
        # This loop must never stop.
        logging.exception('Failed to complete assignment')

    yield gen.sleep(10)


################################
# MAIN
################################
if __name__ == "__main__":
  file_io.set_logging_format()

  zk_ips = appscale_info.get_zk_node_ips()
  zk_client = KazooClient(hosts=','.join(zk_ips))
  zk_client.start()
  deployment_config.init(zk_client)
  thread_pool = ThreadPoolExecutor(4)
  source_manager = SourceManager(zk_client, thread_pool)
  version_manager = VersionManager(zk_client)
  monit_operator = MonitOperator(thread_pool)

  INTERNAL_IP = appscale_info.get_private_ip()
  instance_node = '{}/{}'.format(constants.INSTANCE_ASSIGNMENTS_NODE,
                                 INTERNAL_IP)
  zk_client.DataWatch(instance_node, assignment_watcher)

  io_loop = IOLoop.current()
  io_loop.spawn_callback(watchdog_loop)
  io_loop.start()
