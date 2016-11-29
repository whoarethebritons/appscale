"""
This file contains constants used throughout AppScale.
"""
import os


class HTTPCodes(object):
  BAD_REQUEST = 400
  FORBIDDEN = 403
  NOT_FOUND = 404
  NOT_IMPLEMENTED = 501

# AppScale home directory.
APPSCALE_HOME = os.environ.get("APPSCALE_HOME", "/root/appscale")

# Location of PID files for processes and applications.
APP_PID_DIR = '/etc/appscale/'

# Location of where data is persisted on disk.
APPSCALE_DATA_DIR = '/opt/appscale'

# Location of Java AppServer.
JAVA_APPSERVER = APPSCALE_HOME + '/AppServer_Java'

# The format each service should use for logging.
LOG_FORMAT = '%(asctime)s %(levelname)s %(filename)s:%(lineno)s %(message)s '

# The location of the file which specifies all the ips for this deployment.
ALL_IPS_LOC = '/etc/appscale/all_ips'

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

# The location of the file which contains information on the current DB.
DB_INFO_LOC = '/etc/appscale/database_info.yaml'

# The file location which has all taskqueue nodes listed.
TASKQUEUE_NODE_FILE = "/etc/appscale/taskqueue_nodes"

# The port of the datastore server.
DB_SERVER_PORT = 8888

# The port of the UserAppServer SOAP server.
UA_SERVER_PORT = 4343

# The port of the application manager soap server.
APP_MANAGER_PORT = 17445

# Python programs.
PYTHON = "python"

# Python2.7 programs.
PYTHON27 = "python27"

# Java programs.
JAVA = "java"

# Go programs.
GO = "go"

# PHP programs.
PHP = "php"

# Location where applications are stored.
APPS_PATH = "/var/apps/"

# Locations of ZooKeeper in json format.
ZK_LOCATIONS_JSON_FILE = "/etc/appscale/zookeeper_locations.json"

# Default location for connecting to ZooKeeper.
ZK_DEFAULT_CONNECTION_STR = "localhost:2181"

# Default location for the datastore master.
MASTERS_FILE_LOC = "/etc/appscale/masters"

# Default location for the datastore slaves.
SLAVES_FILE_LOC = "/etc/appscale/slaves"

# Application ID for AppScale Dashboard.
DASHBOARD_APP_ID = "appscaledashboard"

# Reserved application identifiers which are only internal for AppScale.
RESERVED_APP_IDS = [DASHBOARD_APP_ID]

# Location of where the search service is running.
SEARCH_FILE_LOC = "/etc/appscale/search_ip"

# Service scripts directory.
SERVICES_DIR = '/etc/init.d'

# The AppController's service name.
CONTROLLER_SERVICE = 'appscale-controller'

# The default log directory for AppScale services.
LOG_DIR = os.path.join('/var', 'log', 'appscale')

# The number of seconds to wait before retrying some operations.
SMALL_WAIT = 5

# The number of seconds to wait before retrying some operations.
TINY_WAIT = .1
