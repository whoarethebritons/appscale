""" Deletes all application data. """

import logging
import sys

from ..dbconstants import APP_ENTITY_SCHEMA
from ..dbconstants import APP_ENTITY_TABLE
from ..dbconstants import APP_KIND_SCHEMA
from ..dbconstants import APP_KIND_TABLE
from ..dbconstants import ASC_PROPERTY_TABLE
from ..dbconstants import COMPOSITE_SCHEMA
from ..dbconstants import COMPOSITE_TABLE
from ..dbconstants import DSC_PROPERTY_TABLE
from ..dbconstants import METADATA_SCHEMA
from ..dbconstants import METADATA_TABLE
from ..dbconstants import PROPERTY_SCHEMA
from ..dbconstants import TRANSACTIONS_SCHEMA
from ..dbconstants import TRANSACTIONS_TABLE
from ..unpackaged import APPSCALE_LIB_DIR
from ..utils import fetch_and_delete_entities

sys.path.append(APPSCALE_LIB_DIR)
from constants import LOG_FORMAT


def main():
  logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)

  database = "cassandra"
  first_key = ""
  last_key = ""

  if len(sys.argv) > 2:
    print "usage: appscale-delete-all-records [app_id]"
    exit(1)

  if len(sys.argv) == 2:
    first_key = sys.argv[1]

  try:
    tables_to_schemas = {
      APP_ENTITY_TABLE: APP_ENTITY_SCHEMA,
      ASC_PROPERTY_TABLE: PROPERTY_SCHEMA,
      DSC_PROPERTY_TABLE: PROPERTY_SCHEMA,
      COMPOSITE_TABLE: COMPOSITE_SCHEMA,
      APP_KIND_TABLE: APP_KIND_SCHEMA,
      METADATA_TABLE: METADATA_SCHEMA,
      TRANSACTIONS_TABLE: TRANSACTIONS_SCHEMA
    }
    for table, schema in tables_to_schemas.items():
      fetch_and_delete_entities(database, table, schema, first_key, False)
  except:
    raise
