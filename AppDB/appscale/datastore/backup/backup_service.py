""" Class for handling serialized backup/recovery requests. """

import logging
import json
import threading

from appscale.datastore.backup import backup_exceptions
from appscale.datastore.backup import backup_recovery_helper
from appscale.datastore.backup import cassandra_backup
from appscale.datastore.backup.br_constants import StorageTypes


class BackupService():
  """ Backup service class. """

  # The key to use to figure out the type of request sent.
  REQUEST_TYPE_TAG = "type"

  # The key to use to lookup the backup name.
  BACKUP_NAME_TAG = "backup_name"

  # Google Cloud Storage bucket tag name. 
  BUCKET_NAME_TAG = "bucket_name"

  # Google Cloud Storage object tag name.
  OBJECT_NAME_TAG = "object_name"

  # The storage infrastructure used for backups.
  STORAGE = 'storage'

  # AppScale components that can currently be backed up.
  SUPPORTED_COMPONENTS = ['cassandra', 'zookeeper']

  def __init__(self):
    """ Constructor function for the backup service. """
    log_format = logging.Formatter('%(asctime)s %(levelname)s %(filename)s: '
      '%(lineno)s %(message)s')
    logging.getLogger().handlers[0].setFormatter(log_format)
    self.__cassandra_backup_lock = threading.Lock()

  @classmethod
  def bad_request(cls, reason):
    """ Returns the default bad request json string.

    Args:
      reason: The reason the request is bad.
    Returns:
      The default message to return on a bad request.
    """
    return json.dumps({'success': False, 'reason': reason})

  def remote_request(self, request_data):
    """ Handles remote requests with serialized JSON.

    Args:
      request_data: A str, the serialized JSON request.
    Returns:
      A str, serialized JSON.
    """
    try:
      request = json.loads(request_data)
      logging.info("Request received: {0}".format(request))
    except (TypeError, ValueError) as error:
      logging.exception(error)
      return self.bad_request("Unable to parse request. Exception: {0}".
        format(error))

    request_type = request[self.REQUEST_TYPE_TAG]
    if request_type.split('_')[0] not in self.SUPPORTED_COMPONENTS:
      return self.bad_request("Unsupported request type '{0}'".format(request))

    storage = request[self.STORAGE]
    if storage not in StorageTypes().get_storage_types():
      return self.bad_request("Unsupported storage type '{0}'".format(storage))

    path = request[self.OBJECT_NAME_TAG]
    if not path:
      return self.bad_request("Missing path argument.")

    if request_type == "cassandra_backup":
      return self.do_cassandra_backup(storage, path)
    elif request_type == "cassandra_restore":
      return self.do_cassandra_restore(storage, path)
    elif request_type == "app_backup":
      return self.do_app_backup(storage, path)
    elif request_type == "app_restore":
      return self.do_app_restore(storage, path)

  def do_app_backup(self, storage, path):
    """ Top level function for doing source code backups.

    Args:
      storage: A str, one of the StorageTypes class members.
      path: A str, the name of the backup file to be created.
    Returns:
      A JSON string to return to the client.
    """
    if not backup_recovery_helper.app_backup(storage, path):
      return self.bad_request("Source code backup failed!")
    logging.info("Successful source code backup!")
    return json.dumps({'success': True, 'reason': ''})

  def do_app_restore(self, storage, path):
    """ Top level function for restoring source code.

    Args:
      storage: A str, one of the StorageTypes class members.
      path: A str, the name of the backup file to be created.
    Returns:
      A JSON string to return to the client.
    """
    if not backup_recovery_helper.app_restore(storage, path):
      return self.bad_request("Source code restore failed!")
    logging.info("Successful source code restore!")
    return json.dumps({'success': True, 'reason': ''})

  def do_cassandra_backup(self, storage, path):
    """ Top level function for doing Cassandra backups.

    Args:
      storage: A str, one of the StorageTypes class members.
      path: A str, the name of the backup file to be created.
    Returns:
      A JSON string to return to the client.
    """
    success = True
    reason = "success"
    try:
      logging.info("Acquiring lock for db backup.")
      self.__cassandra_backup_lock.acquire(True)
      logging.info("Got the lock for db backup.")
      if not cassandra_backup.backup_data(storage, path):
        return self.bad_request("DB backup failed!")
      else:
        logging.info("Successful db backup!")
    except backup_exceptions.BRException, exception:
      logging.error("Unable to complete db backup: {0}".format(exception))
      success = False
      reason = str(exception)
    finally:
      self.__cassandra_backup_lock.release()

    return json.dumps({'success': success, 'reason': reason})

  def do_cassandra_restore(self, storage, path):
    """ Top level function for doing Cassandra restores.

    Args:
      storage: A str, one of the StorageTypes class members.
      path: A str, the name of the backup file to be created.
    Returns:
      A JSON string to return to the client.
    """
    success = True
    reason = "success"
    try:
      logging.info("Acquiring lock for db restore.")
      self.__cassandra_backup_lock.acquire(True)
      logging.info("Got the lock for db restore.")
      if not cassandra_backup.restore_data(storage, path):
        return self.bad_request("DB restore failed!")
      else:
        logging.info("Successful db restore!")
    except backup_exceptions.BRException, exception:
      logging.error("Unable to complete db restore: {0}".format(exception))
      success = False
      reason = str(exception)
    finally:
      self.__cassandra_backup_lock.release()

    return json.dumps({'success': success, 'reason': reason})
