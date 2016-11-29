"""
 Datastore Constants
"""
import cassandra.cluster

SECRET_LOCATION = "/etc/appscale/secret.key"

ERROR_DEFAULT = "DB_ERROR:"
NONEXISTANT_TRANSACTION = "0"

# The datastore's default HTTP port.
DEFAULT_PORT = 4080

# The datastore's default HTTPS port.
DEFAULT_SSL_PORT = 8443

# The lowest character to separate different fields in a row key.
KEY_DELIMITER = '\x00'

# The character used to separate kinds in an ancestry.
KIND_SEPARATOR = '\x01'

# HTTP code to indicate that the request is invalid.
HTTP_BAD_REQUEST = 400

# The length of an ID string. A constant length allows lexicographic ordering.
ID_KEY_LENGTH = 10

# The character between the kind and the ID/name of an entity.
ID_SEPARATOR = ":"

# The maximum number of composite indexes an application can have.
MAX_NUMBER_OF_COMPOSITE_INDEXES = 1000

# A string used to create end keys when doing range queries.
TERMINATING_STRING = chr(255) * 500

# Tombstone value for soft deletes.
TOMBSTONE = "APPSCALE_SOFT_DELETE"

TRANSIENT_CASSANDRA_ERRORS = (
  cassandra.Unavailable, cassandra.Timeout, cassandra.CoordinationFailure,
  cassandra.OperationTimedOut, cassandra.cluster.NoHostAvailable)

# The database backends supported by the AppScale datastore.
VALID_DATASTORES = ['cassandra']

# Table names
USERS_TABLE = "USERS__"
APPS_TABLE = "APPS__"
JOURNAL_TABLE = "JOURNAL__"

ASC_PROPERTY_TABLE = "ASC_PROPERTY__"
DSC_PROPERTY_TABLE = "DSC_PROPERTY__"
COMPOSITE_TABLE = "COMPOSITE_INDEXES__"
APP_ID_TABLE = "APP_IDS__"
APP_ENTITY_TABLE = "ENTITIES__"
APP_KIND_TABLE = "KINDS__"
METADATA_TABLE = "METADATA__"
DATASTORE_METADATA_TABLE = "DATASTORE_METADATA__"
TRANSACTIONS_TABLE = 'TRANSACTIONS__'
SCHEMA_TABLE = '__key__'

INITIAL_TABLES = [ASC_PROPERTY_TABLE,
                  DSC_PROPERTY_TABLE,
                  APP_ID_TABLE,
                  APP_ENTITY_TABLE,
                  APP_KIND_TABLE,
                  COMPOSITE_TABLE,
                  METADATA_TABLE,
                  USERS_TABLE,
                  APPS_TABLE,
                  SCHEMA_TABLE,
                  DATASTORE_METADATA_TABLE,
                  TRANSACTIONS_TABLE]

###########################################
# DB schemas for version 1 of the datastore
###########################################
JOURNAL_SCHEMA = [
  "Encoded_Entity"]

ENTITY_TABLE_SCHEMA = [
  "Encoded_Entity",
  "Txn_Num"]

###########################################
# DB schema for version 2 of the datastore
###########################################

# The schema of the table which holds the encoded entities
APP_ENTITY_SCHEMA = [
  "entity",
  "txnID"]

# Index tables store references are to entity table
PROPERTY_SCHEMA = [
  "reference" ]
APP_ID_SCHEMA = [
  "next_id" ]
APP_KIND_SCHEMA = [
  "reference" ]
COMPOSITE_SCHEMA = [
  "reference" ]
METADATA_SCHEMA = [
  "data" ]

USERS_SCHEMA = [
  "email",
  "pw",
  "date_creation", 
  "date_change",
  "date_last_login",
  "applications",
  "appdrop_rem_token",
  "appdrop_rem_token_exp",
  "visit_cnt",
  "cookie",
  "cookie_ip",
  "cookie_exp",
  "cksum",
  "enabled",
  "type",
  "is_cloud_admin",
  "capabilities" ]

APPS_SCHEMA = [
  "name",
  "language",
  "version",
  "owner",
  "admins_list",
  "host",
  "port",
  "creation_date",
  "last_time_updated_date",
  "yaml_file",
  "cksum",
  "num_entries",
  "tar_ball",
  "enabled",
  "indexes" ]

DATASTORE_METADATA_SCHEMA = [
  "version"]

TRANSACTIONS_SCHEMA = [
  'operation',
  'operand',
  'exclude_indices'
]

# All schema information for the keyspace is stored in the schema table.
SCHEMA_TABLE_SCHEMA = ['schema']


# Possible values in the 'action' column of the transaction table.
class TxnActions(object):
  DELETE = '0'
  PUT = '1'


###############################
# Generic Datastore Exceptions
###############################
class AppScaleDBError(Exception):
  """ Tossed for generic datastore errors
  """
  def __init__(self, value):
    Exception.__init__(self, value)
    self.value = value

  def __str__(self):
    return repr(self.value)

class AppScaleDBConnectionError(Exception):
  """ Tossed when there is a bad connection
  """ 
  def __init__(self, value):
    Exception.__init__(self, value)
    self.value = value
  def __str__(self):
    return repr(self.value)

class AppScaleMisconfiguredQuery(Exception):
  """ Tossed when a query is misconfigured
  """
  def __init__(self, value):
    Exception.__init__(self, value)
    self.value = value
  def __str__(self):
    return repr(self.value)

class AppScaleBadArg(Exception):
  """ Bad Argument given for a function
  """
  def __init__(self, value):
    Exception.__init__(self, value)
    self.value = value
  def __str__(self):
    return repr(self.value)
