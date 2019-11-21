"""
This file contains constants used throughout AppScale.
"""
import os
from kazoo.retry import KazooRetry


class HTTPCodes(object):
  OK = 200
  BAD_REQUEST = 400
  UNAUTHORIZED = 401
  FORBIDDEN = 403
  NOT_FOUND = 404
  INTERNAL_ERROR = 500
  NOT_IMPLEMENTED = 501


class InvalidIndexConfiguration(Exception):
  """ Indicates that a given configuration cannot be enforced. """
  pass


def non_negative_int(value):
  """ Checks if an integer value is greater or equal than 0. """
  return isinstance(value, int) and value >= 0

# AppScale home directory.
APPSCALE_HOME = os.environ.get("APPSCALE_HOME", "/home/appscale/appscale")

# The ZooKeeper path for keeping track of assignments by machine.
ASSIGNMENTS_PATH = '/appscale/assignments'

# Directory where configuration files are stored.
CONFIG_DIR = os.path.join('/', 'etc', 'appscale')

# The ZooKeeper path where a list of active blobstore servers is stored.
BLOBSTORE_SERVERS_NODE = '/appscale/blobstore/servers'

# The ZooKeeper path where a list of active datastore servers is stored.
DATASTORE_SERVERS_NODE = '/appscale/datastore/servers'

# The ZooKeeper path where a list of active search servers is stored.
SEARCH_SERVERS_NODE = '/appscale/search/servers'

# The ZooKeeper path where a list of active taskqueue servers is stored.
TQ_SERVERS_NODE = '/appscale/tasks/servers'

# The ZooKeeper path where a list of active UA servers is stored.
UA_SERVERS_NODE = '/appscale/iam/servers'

# Location of where data is persisted on disk.
APPSCALE_DATA_DIR = '/opt/appscale'

# Location of Java AppServer.
JAVA_APPSERVER = APPSCALE_HOME + '/AppServer_Java'

# The directory where the python27 runtime is installed.
PYTHON27_RUNTIME_DIR = os.path.join(APPSCALE_HOME, 'AppServer')

# The directory where the java8 runtime is installed.
JAVA8_RUNTIME_DIR = os.path.join('/opt', 'appscale_java8_runtime')

# The format each service should use for logging.
LOG_FORMAT = '%(asctime)s %(levelname)s %(filename)s:%(lineno)s %(message)s '

# The location of the file containing the load balancer IPs.
LOAD_BALANCER_IPS_LOC = '/etc/appscale/load_balancer_ips'

# The location of the file which specifies all the ips for this deployment.
ALL_IPS_LOC = '/etc/appscale/all_ips'

# A prefix used to indicate that a config file is AppServer-related.
GAE_PREFIX = 'gae_'

# The location of the file which specifies the public IP of the head node.
HEADNODE_IP_LOC = '/etc/appscale/head_node_private_ip'

# The directory that contains the deployment's private SSH key.
KEY_DIRECTORY = os.path.join(CONFIG_DIR, 'keys', 'cloud1')

# The location of the file which specifies the public IP of the head node.
LOGIN_IP_LOC = '/etc/appscale/login_ip'

# The size for the random password to be created for the appscalesensor app user.
PASSWORD_SIZE = 6

# The location of the file which specifies the current private IP.
PRIVATE_IP_LOC = '/etc/appscale/my_private_ip'

# The location of the file which specifies the current public IP.
PUBLIC_IP_LOC = '/etc/appscale/my_public_ip'

# The location of the file which holds the AppScale secret key.
SECRET_LOC = '/etc/appscale/secret.key'

# The Cassandra config location in Zookeeper.
ZK_CASSANDRA_CONFIG = "/appscale/config/cassandra"

# The location of the file which contains information on the current DB.
DB_INFO_LOC = '/etc/appscale/database_info.yaml'

# The file location which has all taskqueue nodes listed.
TASKQUEUE_NODE_FILE = "/etc/appscale/taskqueue_nodes"

# The port used for the blobstore service.
BLOBSTORE_PORT = 6106

# The port of the datastore server.
DB_SERVER_PORT = 8888

# The port used for the search service.
SEARCH_SERVICE_PORT = 9999

# The port of the UserAppServer SOAP server.
UA_SERVER_PORT = 4341

# The port of the application manager soap server.
APP_MANAGER_PORT = 17445

# The HAProxy port for the TaskQueue service.
TASKQUEUE_SERVICE_PORT = 17446

# Python programs.
PYTHON = "python"

# Python2.7 programs.
PYTHON27 = "python27"

# Java programs.
JAVA = "java"

JAVA8 = "java8"

# Go programs.
GO = "go"

# PHP programs.
PHP = "php"

# Location where applications are stored.
APPS_PATH = "/var/apps/"

# Locations of ZooKeeper.
ZK_LOCATIONS_FILE = "/etc/appscale/zookeeper_locations"

# Default location for connecting to ZooKeeper.
ZK_DEFAULT_CONNECTION_STR = "localhost:2181"

# A ZooKeeper reconnect policy that never stops retrying to connect.
ZK_PERSISTENT_RECONNECTS = KazooRetry(max_tries=-1, max_delay=30)

# Default location for the datastore master.
MASTERS_FILE_LOC = "/etc/appscale/masters"

# Default location for the datastore slaves.
SLAVES_FILE_LOC = "/etc/appscale/slaves"

# Application ID for AppScale Dashboard.
DASHBOARD_APP_ID = "appscaledashboard"

# Reserved application identifiers which are only internal for AppScale.
RESERVED_APP_IDS = [DASHBOARD_APP_ID]

# The seconds to wait for the schema to settle after changing it.
SCHEMA_CHANGE_TIMEOUT = 120

# Location of where the search service is running.
SEARCH_FILE_LOC = "/etc/appscale/search_ip"

# The AppController's service name.
CONTROLLER_SERVICE = 'appscale-controller'

# The system's cgroup directory.
CGROUP_DIR = os.path.join('/', 'sys', 'fs', 'cgroup')

# The default log directory for AppScale services.
LOG_DIR = os.path.join('/var', 'log', 'appscale')

# The default directory for run-time variable data (eg. pidfiles).
VAR_DIR = os.path.join('/', 'var', 'run', 'appscale')

# A directory that contains miscellaneous helper scripts.
SCRIPTS_DIR = os.path.join(APPSCALE_HOME, 'scripts')

# The number of seconds to wait before retrying some operations.
SMALL_WAIT = 5

# The number of seconds to wait before retrying some operations.
TINY_WAIT = .1

# The character used to separate portions of a complete version string.
# (e.g. guestbook_default_v1)
VERSION_PATH_SEPARATOR = '_'

# The ZooKeeper node that keeps track of running AppServers by version.
VERSION_REGISTRATION_NODE = '/appscale/instances_by_version'
