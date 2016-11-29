""" Utilities for parsing datastore entities. """

from appscale.datastore import dbconstants
from appscale.datastore.dbconstants import JOURNAL_SCHEMA
from appscale.datastore.dbconstants import JOURNAL_TABLE
from appscale.datastore.dbconstants import KEY_DELIMITER
from appscale.datastore.dbconstants import KIND_SEPARATOR
from google.appengine.datastore import entity_pb


def get_root_key_from_entity_key(key):
  """ Extracts the root key from an entity key. We
      remove any excess children from a string to get to
      the root key.

  Args:
    entity_key: A string representing a row key.
  Returns:
    The root key extracted from the row key.
  """
  tokens = key.split(KIND_SEPARATOR)
  return tokens[0] + KIND_SEPARATOR


def get_kind_from_entity_key(entity_key):
  """ Extracts the kind from a key to the entity table.

  Args:
    entity_key: A str representing a row key to the entity table.
  Returns:
    A str representing the kind.
  """
  tokens = entity_key.split(KEY_DELIMITER)
  return tokens[2].split(":")[0]


def fetch_journal_entry(db_access, key):
  """ Fetches the given key from the journal.

  Args:
    db_access: A datastore accessor.
    keys: A str, the key to fetch.
  Returns:
    The entity fetched from the datastore, or None if it was deleted.
  """
  result = db_access.batch_get_entity(JOURNAL_TABLE, [key],
    JOURNAL_SCHEMA)
  if len(result.keys()) == 0:
    return None

  if JOURNAL_SCHEMA[0] in result.keys()[0]:
    ent_string = result[0][JOURNAL_SCHEMA[0]]
    if ent_string == dbconstants.TOMBSTONE:
      return None
    return entity_pb.EntityProto().ParseFromString(ent_string)
  else:
    return None
