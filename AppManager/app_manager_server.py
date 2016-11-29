""" This service starts and stops application servers of a given application. """

import argparse
import fnmatch
import glob
import json
import logging
import math
import os
import shutil
import SOAPpy
import subprocess
import sys
import threading
import time
import urllib2
from xml.etree import ElementTree

from M2Crypto import SSL

sys.path.append(os.path.join(os.path.dirname(__file__), "../lib/"))
import appscale_info
import constants
import file_io
import monit_app_configuration
import monit_interface
import misc
from deployment_config import DeploymentConfig
from deployment_config import ConfigInaccessible
from monit_app_configuration import MONIT_CONFIG_DIR

sys.path.append(os.path.join(os.path.dirname(__file__), '../AppServer'))
from google.appengine.api.appcontroller_client import AppControllerClient

# The amount of seconds to wait for an application to start up.
START_APP_TIMEOUT = 180

# The amount of seconds to wait between checking if an application is up.
BACKOFF_TIME = 1

# The PID number to return when a process did not start correctly
BAD_PID = -1

# Default hourly cron directory.
CRON_HOURLY = '/etc/cron.hourly'

# Default logrotate configuration directory.
LOGROTATE_CONFIG_DIR = '/etc/logrotate.d'

# Max log size for AppScale Dashboard servers.
DASHBOARD_LOG_SIZE = 10 * 1024 * 1024

# Max application server log size in bytes.
APP_LOG_SIZE = 250 * 1024 * 1024

# Required configuration fields for starting an application
REQUIRED_CONFIG_FIELDS = [
  'app_name',
  'app_port',
  'language',
  'load_balancer_ip',
  'xmpp_ip',
  'env_vars',
  'max_memory']

# The web path to fetch to see if the application is up
FETCH_PATH = '/_ah/health_check'

# The app ID of the AppScale Dashboard.
APPSCALE_DASHBOARD_ID = "appscaledashboard"

# Apps which can access any application's data.
TRUSTED_APPS = ["appscaledashboard"]

# The flag to tell the application server that this application can access
# all application data.
TRUSTED_FLAG = "--trusted"

# The location on the filesystem where the PHP executable is installed.
PHP_CGI_LOCATION = "/usr/bin/php-cgi"

# Load balancing path for datastore.
DATASTORE_PATH = "localhost"

HTTP_OK = 200

# The amount of seconds to wait before retrying to add routing.
ROUTING_RETRY_INTERVAL = 5

# A DeploymentConfig accessor.
deployment_config = None


class BadConfigurationException(Exception):
  """ An application is configured incorrectly. """
  def __init__(self, value):
    Exception.__init__(self, value)
    self.value = value

  def __str__(self):
    return repr(self.value)

class NoRedirection(urllib2.HTTPErrorProcessor):
  """ A url opener that does not automatically redirect. """
  def http_response(self, request, response):
    """ Processes HTTP responses.

    Args:
      request: An HTTP request object.
      response: An HTTP response object.
    Returns:
      The HTTP response object.
    """
    return response
  https_response = http_response

def convert_config_from_json(config):
  """ Takes the configuration in JSON format and converts it to a dictionary.
      Validates the dictionary configuration before returning.

  Args:
    config: The configuration to convert
  Returns:
    None if it failed to convert the config and a dictionary if it succeeded
  """
  logging.info("Configuration for app:" + str(config))
  try:
    config = json.loads(config)
  except ValueError, error:
    logging.error("%s Exception--Unable to parse configuration: %s" % \
      (error.__class__, str(error)))
    return None
  except TypeError, error:
    logging.error("%s Exception--Unable to parse configuration: %s" % \
      (error.__class__, str(error)))
    return None

  if is_config_valid(config):
    return config
  else:
    return None

def add_routing(app, port):
  """ Tells the AppController to begin routing traffic to an AppServer.

  Args:
    app: A string that contains the application ID.
    port: A string that contains the port that the AppServer listens on.
  """
  acc = appscale_info.get_appcontroller_client()
  appserver_ip = appscale_info.get_private_ip()

  while True:
    result = acc.add_routing_for_appserver(app, appserver_ip, port)
    if result == AppControllerClient.NOT_READY:
      logging.info('AppController not yet ready to add routing.')
      time.sleep(ROUTING_RETRY_INTERVAL)
    else:
      break

  logging.info('Successfully established routing for {} on port {}'.
    format(app, port))

def remove_routing(app, port):
  """ Tells the AppController to stop routing traffic to an AppServer.

  Args:
    app: A string that contains the application ID.
    port: A string that contains the port that the AppServer listens on.
  """
  acc = appscale_info.get_appcontroller_client()
  appserver_ip = appscale_info.get_private_ip()
  acc.remove_appserver_from_haproxy(app, appserver_ip, port)

def start_app(config):
  """ Starts a Google App Engine application on this machine. It
      will start it up and then proceed to fetch the main page.

  Args:
    config: a dictionary that contains
       app_name: Name of the application to start
       app_port: Port to start on
       language: What language the app is written in
       load_balancer_ip: Public ip of load balancer
       xmpp_ip: IP of XMPP service
       env_vars: A dict of environment variables that should be passed to the
        app.
       max_memory: An int that names the maximum amount of memory that this
        App Engine app is allowed to consume before being restarted.
       syslog_server: The IP of the syslog server to send the application
         logs to. Usually it's the login private IP.
  Returns:
    PID of process on success, -1 otherwise
  """
  config = convert_config_from_json(config)
  if config is None:
    logging.error("Invalid configuration for application")
    return BAD_PID

  if not misc.is_app_name_valid(config['app_name']):
    logging.error("Invalid app name for application: " + config['app_name'])
    return BAD_PID
  logging.info("Starting %s application %s" % (
    config['language'], config['app_name']))

  env_vars = config['env_vars']
  env_vars['GOPATH'] = '/root/appscale/AppServer/gopath/'
  env_vars['GOROOT'] = '/root/appscale/AppServer/goroot/'
  watch = "app___" + config['app_name']

  if config['language'] == constants.PYTHON27 or \
      config['language'] == constants.GO or \
      config['language'] == constants.PHP:
    start_cmd = create_python27_start_cmd(
      config['app_name'],
      config['load_balancer_ip'],
      config['app_port'],
      config['load_balancer_ip'],
      config['xmpp_ip'])
    stop_cmd = create_python27_stop_cmd(config['app_port'])
    env_vars.update(create_python_app_env(
      config['load_balancer_ip'],
      config['app_name']))
  elif config['language'] == constants.JAVA:
    remove_conflicting_jars(config['app_name'])
    copy_successful = copy_modified_jars(config['app_name'])
    if not copy_successful:
      return BAD_PID

    # Account for MaxPermSize (~170MB), the parent process (~50MB), and thread
    # stacks (~20MB).
    max_heap = config['max_memory'] - 250
    if max_heap <= 0:
      return BAD_PID
    start_cmd = create_java_start_cmd(
      config['app_name'],
      config['app_port'],
      config['load_balancer_ip'],
      max_heap
    )

    stop_cmd = create_java_stop_cmd(config['app_port'])
    env_vars.update(create_java_app_env(config['app_name']))
  else:
    logging.error("Unknown application language %s for appname %s" \
      % (config['language'], config['app_name']))
    return BAD_PID

  logging.info("Start command: " + str(start_cmd))
  logging.info("Stop command: " + str(stop_cmd))
  logging.info("Environment variables: " + str(env_vars))

  # Set the syslog_server is specified.
  syslog_server = ""
  if 'syslog_server' in config:
    syslog_server = config['syslog_server']
  monit_app_configuration.create_config_file(
    str(watch),
    str(start_cmd),
    str(stop_cmd),
    [config['app_port']],
    env_vars,
    config['max_memory'],
    syslog_server,
    appscale_info.get_private_ip())

  if not monit_interface.start(watch):
    logging.error("Unable to start application server with monit")
    return BAD_PID

  if not wait_on_app(int(config['app_port'])):
    logging.error("Application server did not come up in time, "
      "removing monit watch")
    monit_interface.stop(watch)
    return BAD_PID

  threading.Thread(target=add_routing,
    args=(config['app_name'], config['app_port'])).start()

  if 'log_size' in config.keys():
    log_size = config['log_size']
  else:
    if config['app_name'] == APPSCALE_DASHBOARD_ID:
      log_size = DASHBOARD_LOG_SIZE
    else:
      log_size = APP_LOG_SIZE

  if not setup_logrotate(config['app_name'], watch, log_size):
    logging.error("Error while setting up log rotation for application: {}".
      format(config['app_name']))


  return 0

def setup_logrotate(app_name, watch, log_size):
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
  app_logrotate_script = "{0}/appscale-{1}".\
    format(LOGROTATE_CONFIG_DIR, app_name)

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
  logging.debug("Logrotate file: {} - Contents:\n{}".
    format(app_logrotate_script, contents))

  with open(app_logrotate_script, 'w') as app_logrotate_fd:
    app_logrotate_fd.write(contents)

  return True

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

  logging.info('Removing routing for {} on port {}'.format(app_name, port))
  remove_routing(app_name, port)

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

def restart_app_instances_for_app(app_name, language):
  """ Restarts all instances of a Google App Engine application on this machine.

  Args:
    app_name: The application ID corresponding to the app to restart.
    language: The language the application is written in.
  Returns:
    True if successful, and False otherwise.
  """
  if not misc.is_app_name_valid(app_name):
    logging.error("Unable to kill app process %s on because of " \
      "invalid name for application" % (app_name))
    return False
  if language == "java":
    remove_conflicting_jars(app_name)
    copy_modified_jars(app_name)
  logging.info("Restarting application %s" % app_name)
  watch = "app___" + app_name
  return monit_interface.restart(watch)

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

############################################
# Private Functions (but public for testing)
############################################
def wait_on_app(port):
  """ Waits for the application hosted on this machine, on the given port,
      to respond to HTTP requests.

  Args:
    port: Port where app is hosted on the local machine
  Returns:
    True on success, False otherwise
  """
  retries = math.ceil(START_APP_TIMEOUT / BACKOFF_TIME)
  private_ip = appscale_info.get_private_ip()

  url = "http://" + private_ip + ":" + str(port) + FETCH_PATH
  while retries > 0:
    try:
      opener = urllib2.build_opener(NoRedirection)
      response = opener.open(url)
      if response.code != HTTP_OK:
        logging.warning('{} returned {}. Headers: {}'.
          format(url, response.code, response.headers.headers))
      return True
    except IOError:
      retries -= 1

    time.sleep(BACKOFF_TIME)

  logging.error('Application did not come up on {} after {} seconds'.
    format(url, START_APP_TIMEOUT))
  return False

def create_python_app_env(public_ip, app_name):
  """ Returns the environment variables the python application server uses.

  Args:
    public_ip: The public IP of the load balancer
    app_name: The name of the application to be run
  Returns:
    A dictionary containing the environment variables
  """
  env_vars = {}
  env_vars['MY_IP_ADDRESS'] = public_ip
  env_vars['APPNAME'] = app_name
  env_vars['GOMAXPROCS'] = appscale_info.get_num_cpus()
  env_vars['APPSCALE_HOME'] = constants.APPSCALE_HOME
  env_vars['PYTHON_LIB'] = "{0}/AppServer/".format(constants.APPSCALE_HOME)
  return env_vars

def find_web_xml(app_name):
  """ Returns the location of a Java application's appengine-web.xml file.

  Args:
    app_name: A string containing the application ID.
  Returns:
    A string containing the location of the file.
  Raises:
    BadConfigurationException if the file is not found or multiple candidates
    are found.

  """
  app_dir = '/var/apps/{}/app'.format(app_name)
  file_name = 'appengine-web.xml'
  matches = []
  for root, dirs, files in os.walk(app_dir):
    if file_name in files and root.endswith('/WEB-INF'):
      matches.append(os.path.join(root, file_name))

  if len(matches) < 1:
    raise BadConfigurationException(
      'Unable to find {} file for {}'.format(file_name, app_name))
  if len(matches) > 1:
    # Use the shortest path. If there are any ties, use the first after
    # sorting alphabetically.
    matches.sort()
    match_to_use = matches[0]
    for match in matches:
      if len(match) < len(match_to_use):
        match_to_use = match
    return match_to_use
  return matches[0]

def extract_env_vars_from_xml(xml_file):
  """ Returns any custom environment variables defined in appengine-web.xml.

  Args:
    xml_file: A string containing the location of the xml file.
  Returns:
    A dictionary containing the custom environment variables.
  """
  custom_vars = {}
  tree = ElementTree.parse(xml_file)
  root = tree.getroot()
  for child in root:
    if not child.tag.endswith('env-variables'):
      continue

    for env_var in child:
      var_dict = env_var.attrib
      custom_vars[var_dict['name']] = var_dict['value']

  return custom_vars

def create_java_app_env(app_name):
  """ Returns the environment variables Java application servers uses.

  Args:
    app_name: A string containing the application ID.
  Returns:
    A dictionary containing the environment variables
  """
  env_vars = {'APPSCALE_HOME': constants.APPSCALE_HOME}

  config_file = find_web_xml(app_name)
  custom_env_vars = extract_env_vars_from_xml(config_file)
  env_vars.update(custom_env_vars)

  gcs_config = {'scheme': 'https', 'port': 443}
  try:
    gcs_config.update(deployment_config.get_config('gcs'))
  except ConfigInaccessible:
    logging.warning('Unable to fetch GCS configuration.')

  if 'host' in gcs_config:
    env_vars['GCS_HOST'] = '{scheme}://{host}:{port}'.format(**gcs_config)

  return env_vars

def create_python27_start_cmd(app_name,
  login_ip, port, load_balancer_host, xmpp_ip):
  """ Creates the start command to run the python application server.

  Args:
    app_name: The name of the application to run
    login_ip: The public IP
    port: The local port the application server will bind to
    load_balancer_host: The host of the load balancer
    xmpp_ip: The IP of the XMPP service
  Returns:
    A string of the start command.
  """
  db_location = DATASTORE_PATH
  cmd = [
    "/usr/bin/python2",
    constants.APPSCALE_HOME + "/AppServer/dev_appserver.py",
    "--port " + str(port),
    "--admin_port " + str(port + 10000),
    "--login_server " + login_ip,
    "--skip_sdk_update_check",
    "--nginx_host " + str(load_balancer_host),
    "--require_indexes",
    "--enable_sendmail",
    "--xmpp_path " + xmpp_ip,
    "--php_executable_path=" + str(PHP_CGI_LOCATION),
    "--uaserver_path " + db_location + ":"\
      + str(constants.UA_SERVER_PORT),
    "--datastore_path " + db_location + ":"\
      + str(constants.DB_SERVER_PORT),
    "/var/apps/" + app_name + "/app",
    "--host " + appscale_info.get_private_ip()]

  if app_name in TRUSTED_APPS:
    cmd.extend([TRUSTED_FLAG])

  return ' '.join(cmd)

def locate_dir(path, dir_name):
  """ Locates a directory inside the given path.

  Args:
    path: The path to be searched
    dir_name: The directory we are looking for

  Returns:
    The absolute path of the directory we are looking for, None otherwise.
  """
  paths = []

  for root, sub_dirs, files in os.walk(path):
    for sub_dir in sub_dirs:
      if dir_name == sub_dir:
        result = os.path.abspath(os.path.join(root, sub_dir))
        if sub_dir == "WEB-INF":
          logging.info("Found WEB-INF/ at: {0}".format(result))
          paths.append(result)
        elif sub_dir == "lib" and result.count(os.sep) <= path.count(os.sep) + 2 \
            and result.endswith("/WEB-INF/{0}".format(sub_dir)):
          logging.info("Found lib/ at: {0}".format(result))
          paths.append(result)

  if len(paths) > 0:
    sorted_paths = sorted(paths, key = lambda s: len(s))
    return sorted_paths[0]
  else:
    return None

def remove_conflicting_jars(app_name):
  """ Removes jars uploaded which may conflict with AppScale jars.

  Args:
    app_name: The name of the application to run.
  """
  app_dir = "/var/apps/" + app_name + "/app/"
  lib_dir = locate_dir(app_dir, "lib")
  if not lib_dir:
    logging.warn("Lib directory not found in app code while updating.")
    return
  logging.info("Removing jars from {0}".format(lib_dir))
  conflicting_jars_pattern = ['appengine-api-1.0-sdk-*.jar', 'appengine-api-stubs-*.jar',
                  'appengine-api-labs-*.jar', 'appengine-jsr107cache-*.jar',
                  'jsr107cache-*.jar', 'appengine-mapreduce*.jar',
                  'appengine-pipeline*.jar', 'appengine-gcs-client*.jar']
  for file in os.listdir(lib_dir):
    for pattern in conflicting_jars_pattern:
      if fnmatch.fnmatch(file, pattern):
        os.remove(lib_dir + os.sep + file)

def copy_modified_jars(app_name):
  """ Copies the changes made to the Java SDK
  for AppScale into the apps lib folder.

  Args:
    app_name: The name of the application to run

  Returns:
    False if there were any errors, True if success
  """
  appscale_home = constants.APPSCALE_HOME

  app_dir = "/var/apps/" + app_name + "/app/"
  lib_dir = locate_dir(app_dir, "lib")

  if not lib_dir:
    web_inf_dir = locate_dir(app_dir, "WEB-INF")
    lib_dir = web_inf_dir + os.sep + "lib"
    logging.info("Creating lib directory at: {0}".format(lib_dir))
    mkdir_result = subprocess.call("mkdir " + lib_dir, shell=True)

    if mkdir_result != 0:
      logging.error("Failed to create missing lib directory in: {0}.".
        format(web_inf_dir))
      return False
  try:
    copy_files_matching_pattern(appscale_home + "/AppServer_Java/" +\
                "appengine-java-sdk-repacked/lib/user/*.jar", lib_dir)
    copy_files_matching_pattern(appscale_home + "/AppServer_Java/" +\
                "appengine-java-sdk-repacked/lib/impl/appscale-*.jar", lib_dir)
    copy_files_matching_pattern("/usr/share/appscale/ext/*", lib_dir)
  except IOError as io_error:
    logging.error("Failed to copy modified jar files to lib directory of " + app_name +\
                  " due to:" + str(io_error))
    return False
  return True

def copy_files_matching_pattern(file_path_pattern, dest):
  """ Copies files matching the specified pattern to the destination directory.
  Args:
      file_path_pattern: The pattern of the files to be copied over.
      dest: The destination directory.
  """
  for file in glob.glob(file_path_pattern):
    shutil.copy(file, dest)

def create_java_start_cmd(app_name, port, load_balancer_host, max_heap):
  """ Creates the start command to run the java application server.

  Args:
    app_name: The name of the application to run
    port: The local port the application server will bind to
    load_balancer_host: The host of the load balancer
    max_heap: An integer specifying the max heap size in MB.
  Returns:
    A string of the start command.
  """
  db_location = DATASTORE_PATH

  # The Java AppServer needs the NGINX_PORT flag set so that it will read the
  # local FS and see what port it's running on. The value doesn't matter.
  cmd = [
    "cd " + constants.JAVA_APPSERVER + " &&",
    "./appengine-java-sdk-repacked/bin/dev_appserver.sh",
    "--port=" + str(port),
    #this jvm flag allows javax.email to connect to the smtp server
    "--jvm_flag=-Dsocket.permit_connect=true",
    '--jvm_flag=-Xmx{}m'.format(max_heap),
    "--disable_update_check",
    "--address=" + appscale_info.get_private_ip(),
    "--datastore_path=" + db_location,
    "--login_server=" + load_balancer_host,
    "--appscale_version=1",
    "--APP_NAME=" + app_name,
    "--NGINX_ADDRESS=" + load_balancer_host,
    "--NGINX_PORT=anything",
    os.path.dirname(locate_dir("/var/apps/" + app_name +"/app/", "WEB-INF"))
  ]

  return ' '.join(cmd)

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

def create_java_stop_cmd(port):
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
    "java {1}".format(constants.APPSCALE_HOME, port)
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


################################
# MAIN
################################
if __name__ == "__main__":
  file_io.set_logging_format()
  deployment_config = DeploymentConfig(appscale_info.get_zk_locations_string())

  INTERNAL_IP = appscale_info.get_private_ip()
  SERVER = SOAPpy.SOAPServer((INTERNAL_IP, constants.APP_MANAGER_PORT))

  SERVER.registerFunction(start_app)
  SERVER.registerFunction(stop_app)
  SERVER.registerFunction(stop_app_instance)
  SERVER.registerFunction(restart_app_instances_for_app)

  while 1:
    try:
      SERVER.serve_forever()
    except SSL.SSLError:
      pass
