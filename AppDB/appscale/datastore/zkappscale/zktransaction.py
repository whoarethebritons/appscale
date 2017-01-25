#!/usr/bin/python
"""
Distributed id and lock service for transaction support.
"""
import kazoo.client
import kazoo.exceptions
import logging
import os
import re
import sys
import threading
import time
import urllib

from ..cassandra_env import cassandra_interface
from ..dbconstants import (MAX_GROUPS_FOR_XG,
                           MAX_TX_DURATION)
from ..unpackaged import APPSCALE_PYTHON_APPSERVER

from kazoo.exceptions import (NoNodeError,
                              KazooException,
                              ZookeeperError)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import entity_pb


class ZKTimeoutException(Exception):
  """ A special Exception class that should be thrown if a function is 
  taking longer than expected by the caller to run
  """
  pass


class BatchInProgress(Exception):
  """ Indicates that a concurrent process is working on a batch. """
  pass


# A list that indicates that the Zookeeper node to create should be readable
# and writable by anyone.
ZOO_ACL_OPEN = None

# The number of seconds to wait between invocations of the transaction
# garbage collector.
GC_INTERVAL = 30

# The default port that ZooKeeper runs on.
DEFAULT_PORT = 2181

# The host and port that the Zookeeper service runs on, if none is provided.
DEFAULT_HOST = 'localhost:{}'.format(DEFAULT_PORT)

# The value that we should set for paths whose value we don't care about.
DEFAULT_VAL = "default"

# Paths are separated by this for the tree structure in zookeeper.
PATH_SEPARATOR = "/"

# This is the path which contains the different application's lock meta-data.
APPS_PATH = "/appscale/apps"

# This path contains different transaction IDs.
APP_TX_PATH = "txids"

# This is the node which holds all the locks of an application.
APP_LOCK_PATH = "locks"

APP_ID_PATH = "ids"

APP_TX_PREFIX = "tx"

APP_LOCK_PREFIX = "lk"

APP_ID_PREFIX = "id"

# This is the prefix of all keys which have been updated within a transaction.
TX_UPDATEDKEY_PREFIX = "ukey"

# This is the name of the leaf. It holds a list of locks as a string.
TX_LOCK_PATH = "lockpath"

# The path for blacklisted transactions.
TX_BLACKLIST_PATH = "blacklist"

# This is the path name for valid versions of entities used in a transaction.
TX_VALIDLIST_PATH = "validlist"

GC_LOCK_PATH = "gclock"

GC_TIME_PATH = "gclast_time"

# Lock path for the datastore groomer.
DS_GROOM_LOCK_PATH = "/appscale_datastore_groomer"

# Lock path for the datastore backup.
DS_BACKUP_LOCK_PATH = "/appscale_datastore_backup"

# Lock path for the datastore backup.
DS_RESTORE_LOCK_PATH = "/appscale_datastore_restore"

# A unique prefix for cross group transactions.
XG_PREFIX = "xg"

# The separator value for the lock list when using XG transactions.
LOCK_LIST_SEPARATOR = "!XG_LIST!"

# The location of the ZooKeeper server script.
ZK_SERVER_CMD_LOCATIONS = [
  os.path.join('/usr', 'share', 'zookeeper', 'bin', 'zkServer.sh'),
  os.path.join('/usr', 'lib', 'zookeeper', 'bin', 'zkServer.sh')
]

class ZKTransactionException(Exception):
  """ ZKTransactionException defines a custom exception class that should be
  thrown whenever there was a problem involving a transaction (e.g., the
  transaction failed, we couldn't get a transaction ID).
  """
  pass

class ZKInternalException(Exception):
  """ ZKInternalException defines a custom exception class that should be
  thrown whenever we cannot connect to ZooKeeper for an extended amount of time.
  """
  pass

class ZKBadRequest(ZKTransactionException):
  """ A class thrown when there are too many locks acquired in a XG transaction
  or when XG operations are done on a non XG transaction.
  """
  pass

class ZKTransaction:
  """ ZKTransaction provides an interface that can be used to acquire locks
  and other functions needed to perform database-agnostic transactions
  (e.g., releasing locks, keeping track of transaction metadata).
  """

  # The number of times we should retry ZooKeeper operations, by default.
  DEFAULT_NUM_RETRIES = 0

  # How long to wait before retrying an operation.
  ZK_RETRY_TIME = .5

  # The number of seconds to wait before we consider a zk call a failure.
  DEFAULT_ZK_TIMEOUT = 3

  # When we have this many failures trying to connect to ZK, abort execution.
  MAX_CONNECTION_FAILURES = 10 

  def __init__(self, host=DEFAULT_HOST, start_gc=False, db_access=None,
               log_level=logging.INFO):
    """ Creates a new ZKTransaction, which will communicate with Zookeeper
    on the given host.

    Args:
      host: A str that indicates which machine runs the Zookeeper service.
      start_gc: A bool that indicates if we should start the garbage collector
        for timed out transactions.
      db_access: A DatastoreProxy instance.
      log_level: A logging constant that specifies the instance logging level.
    """
    class_name = self.__class__.__name__
    self.logger = logging.getLogger(class_name)
    self.logger.setLevel(log_level)
    self.logger.info('Starting {}'.format(class_name))

    # Connection instance variables.
    self.needs_connection = True
    self.failure_count = 0
    self.host = host
    self.handle = kazoo.client.KazooClient(hosts=host,
      max_retries=self.DEFAULT_NUM_RETRIES, timeout=self.DEFAULT_ZK_TIMEOUT)
    self.run_with_retry = self.handle.retry
    try:
      self.handle.start()
      self.needs_connection = False
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()

    self.__counter_cache = {}

    # for gc
    self.gc_running = False
    self.gc_cv = threading.Condition()
    if start_gc:
      self.start_gc()

    self.db_access = db_access

  def start_gc(self):
    """ Starts a new thread that cleans up failed transactions.

    If called when the GC thread is already started, this causes the GC thread
    to reload its GC settings.
    """
    self.logger.info("Starting GC thread")
    with self.gc_cv:
      if self.gc_running:
        self.gc_cv.notifyAll()
      else:
        self.gc_running = True
        self.gcthread = threading.Thread(target=self.gc_runner)
        self.gcthread.start()

  def stop_gc(self):
    """ Stops the thread that cleans up failed transactions.
    """
    self.logger.info("Stopping GC thread")
    if self.gc_running:
      with self.gc_cv:
        self.gc_running = False
        self.gc_cv.notifyAll()
      self.gcthread.join()
      self.logger.info("GC is done")

  def close(self):
    """ Stops the thread that cleans up failed transactions and closes its
    connection to Zookeeper.
    """
    self.logger.info("Closing ZK connection")
    self.stop_gc()
    self.handle.stop()
    self.handle.close()

  def increment_and_get_counter(self, path, value):
    """ Increment a counter atomically.

    Args:
      path: A str of unique path to the counter.
      value: An int of how much to increment the counter by.
    Returns:
      A tuple (int, int) of the previous value and the new value.
    Raises:
      ZKTransactionException: If it could not increment the counter.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    def clear_counter_from_cache():
      """ Deletes a counter from the cache due to an exception being raised.
      """
      if path in self.__counter_cache:
        del self.__counter_cache[path]

    try: 
      counter = None
      if path in self.__counter_cache:
        counter = self.__counter_cache[path]
      else:
        counter = self.handle.Counter(path)
        self.__counter_cache[path] = counter

      counter.__add__(value) 
      new_value = counter.value
      return new_value - value, new_value
    except kazoo.exceptions.ZookeeperError as zoo_exception:
      self.logger.exception(zoo_exception)
      clear_counter_from_cache()
      raise ZKTransactionException("Couldn't increment path {0} by value {1}" \
        .format(path, value))
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      clear_counter_from_cache()
      raise ZKTransactionException(
        "Couldn't increment path {0} with value {1}" \
        .format(path, value))

  def get_node(self, path, retries=5):
    """ Fetch the ZooKeeper node at the given path.

    Args:
      path: A PATH_SEPARATOR-separated str that represents the node whose value
        should be updated.
      retries: The number of times to retry fetching the node.
    Returns:
      The value of the node.
    Raises:
      ZKInternalException: If there was an error trying to fetch the node.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    try:
      return self.run_with_retry(self.handle.get, path)
    except kazoo.exceptions.NoNodeError:
      return False
    except kazoo.exceptions.ZookeeperError as zoo_exception:
      self.logger.exception(zoo_exception)
      if retries > 0:
        self.logger.info('Trying again to fetch node {} with retry #{}'
          .format(path, retries))
        time.sleep(self.ZK_RETRY_TIME)
        return self.get_node(path, retries=retries - 1)
      raise ZKInternalException('Unable to fetch node {}'.format(path))
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      if retries > 0:
        self.logger.info('Trying again to fetch node {} with retry #{}'
          .format(path, retries))
        time.sleep(self.ZK_RETRY_TIME)
        return self.get_node(path, retries=retries - 1)
      raise ZKInternalException('Unable to fetch node {}'.format(path))

  def update_node(self, path, value):
    """ Sets the ZooKeeper node at path to value, creating the node if it
      doesn't exist.

    Args:
      path: A PATH_SEPARATOR-separated str that represents the node whose value
        should be updated.
      value: A str representing the value that should be associated with the
        updated node.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    self.logger.debug(
      'Updating node at {}, with new value {}'.format(path, value))
    try:
      self.run_with_retry(self.handle.set, path, str(value))
    except kazoo.exceptions.NoNodeError:
      try:
        self.run_with_retry(self.handle.create, path, str(value), ZOO_ACL_OPEN,
          makepath=True)
      except kazoo.exceptions.KazooException as kazoo_exception:
        self.logger.exception(kazoo_exception)
        self.reestablish_connection()
    except kazoo.exceptions.ZookeeperError as zoo_exception:
      self.logger.exception(zoo_exception)
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()

  def delete_recursive(self, path):
    """ Deletes the ZooKeeper node at path, and any child nodes it may have.

    Args:
      path: A PATH_SEPARATOR-separated str that represents the node to delete.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    try:
      children = self.run_with_retry(self.handle.get_children, path)
      for child in children:
        self.delete_recursive(PATH_SEPARATOR.join([path, child]))
      self.run_with_retry(self.handle.delete, path)
    except kazoo.exceptions.NoNodeError:
      pass
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()

  def dump_tree(self, path):
    """ Prints information about the given ZooKeeper node and its children.

    Args:
      path: A PATH_SEPARATOR-separated str that represents the node to print
        info about.
    """
    try:
      value = self.run_with_retry(self.handle.get, path)[0]
      self.logger.info("{0} = \"{1}\"".format(path, value))
      children = self.run_with_retry(self.handle.get_children, path)
      for child in children:
        self.dump_tree(PATH_SEPARATOR.join([path, child]))
    except kazoo.exceptions.NoNodeError:
      self.logger.info("{0} does not exist.".format(path))
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()

  def get_app_root_path(self, app_id):
    """ Returns the ZooKeeper path that holds all information for the given
      application.

    Args:
      app_id: A str that represents the application we wish to get the root
        path for.
    Returns:
      A str that represents a ZooKeeper node, whose immediate children are
      the transaction prefix path and the locks prefix path.
    """
    return PATH_SEPARATOR.join([APPS_PATH, urllib.quote_plus(app_id)])

  def get_transaction_prefix_path(self, app_id):
    """ Returns the location of the ZooKeeper node who contains all transactions
    in progress for the given application.

    Args:
      app_id: A str that represents the application we wish to get all
        transaction information for.
    Returns:
      A str that represents a ZooKeeper node, whose immediate children are all
      of the transactions currently in progress.
    """
    return PATH_SEPARATOR.join([self.get_app_root_path(app_id), APP_TX_PATH])

  def get_txn_path_before_getting_id(self, app_id):
    """ Returns a path that callers can use to get new transaction IDs from
    ZooKeeper, which are given as sequence nodes.

    Args:
      app_id: A str that represents the application we wish to build a new
        transaction path for.
    Returns: A str that can be used to create new transactions.
    """
    return PATH_SEPARATOR.join([self.get_transaction_prefix_path(app_id),
      APP_TX_PREFIX])

  def get_transaction_path(self, app_id, txid):
    """ Returns the location of the ZooKeeper node who contains all information
      for a transaction, and is the parent of the transaction lock list and
      registered keys for the transaction.

    Args:
      app_id: A str that represents the application we wish to get the prefix
        path for.
      txid: An int that represents the transaction ID whose path we wish to
        acquire.
    """
    txstr = APP_TX_PREFIX + "%010d" % txid
    return PATH_SEPARATOR.join([self.get_app_root_path(app_id), APP_TX_PATH,
      txstr])

  def get_transaction_lock_list_path(self, app_id, txid):
    """ Returns the location of the ZooKeeper node whose value is a
    XG_LIST-separated str, representing all of the locks that have been acquired
    for the given transaction ID.

    Args:
      app_id: A str that represents the application we wish to get the
        transaction information about.
      txid: A str that represents the transaction ID we wish to get the lock
        list location for.
    Returns:
      A PATH_SEPARATOR-delimited str corresponding to the ZooKeeper node that
      contains the list of locks that have been taken for the given transaction.
    """
    return PATH_SEPARATOR.join([self.get_transaction_path(app_id, txid),
      TX_LOCK_PATH])

  def get_blacklist_root_path(self, app_id):
    """ Returns the location of the ZooKeeper node whose children are
      all of the blacklisted transaction IDs for the given application ID.

    Args:
      app_id: A str corresponding to the application who we want to get
        blacklisted transaction IDs for.
    Returns:
      A str corresponding to the ZooKeeper node whose children are blacklisted
      transaction IDs.
    """
    return PATH_SEPARATOR.join([self.get_transaction_prefix_path(app_id),
      TX_BLACKLIST_PATH])

  def get_valid_transaction_root_path(self, app_id):
    """ Returns the location of the ZooKeeper node whose children are
      all of the valid transaction IDs for the given application ID.

    Args:
      app_id: A str corresponding to the application who we want to get
        valid transaction IDs for.
    Returns:
      A str corresponding to the ZooKeeper node whose children are valid
      transaction IDs.
    """
    return PATH_SEPARATOR.join([self.get_transaction_prefix_path(app_id),
      TX_VALIDLIST_PATH])

  def get_valid_transaction_path(self, app_id, entity_key):
    """ Gets the valid transaction path with the entity key.
    Args:
      app_id: The application ID.
      entity_key: The entity within the path.
    Returns:
      A str representing the transaction path.
    """
    return PATH_SEPARATOR.join([self.get_valid_transaction_root_path(app_id),
      urllib.quote_plus(entity_key)])

  def get_lock_root_path(self, app_id, key):
    """ Gets the root path of the lock for a particular app. 
    
    Args:
      app_id: The application ID.
      key: The key for which we're getting the root path lock.
    Returns: 
      A str of the root lock path.
    """
    return PATH_SEPARATOR.join([self.get_app_root_path(app_id), APP_LOCK_PATH,
      urllib.quote_plus(key)])

  def get_xg_path(self, app_id, tx_id):
    """ Gets the XG path for a transaction.
  
    Args:
      app_id: The application ID whose XG path we want.
      tx_id: The transaction ID whose XG path we want.
    Returns:
      A str representing the XG path for the given transaction.
    """ 
    txstr = APP_TX_PREFIX + "%010d" % tx_id
    return PATH_SEPARATOR.join([self.get_app_root_path(app_id), APP_TX_PATH, 
      txstr, XG_PREFIX])
 
  def create_node(self, path, value):
    """ Creates a new node in ZooKeeper, with the given value.

    Args:
      path: The path to create the node at.
      value: The value that we should store in the node.
    Raises:
      ZKTransactionException: If the sequence node couldn't be created.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    try:
      self.run_with_retry(self.handle.create, path, value=str(value), 
        acl=ZOO_ACL_OPEN, ephemeral=False, sequence=False, makepath=True)
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()
      raise ZKTransactionException("Couldn't create path {0} with value {1} " \
        .format(path, value))

  def create_sequence_node(self, path, value):
    """ Creates a new sequence node in ZooKeeper, with a non-zero initial ID.

    We avoid using zero as the initial ID because Google App Engine apps can
    use a zero ID as a sentinel value, to indicate that an ID should be
    allocated for them.

    Args:
      path: The prefix to create the sequence node at. For example, a prefix
        of '/abc' would result in a sequence node of '/abc1' being created.
      value: The value that we should store in the sequence node.
    Returns:
      A long that represents the sequence ID.    
    Raises:
      ZKTransactionException: If the sequence node couldn't be created.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    try:
      txn_id_path = self.run_with_retry(self.handle.create, path, 
        value=str(value), acl=ZOO_ACL_OPEN, ephemeral=False, sequence=True,
        makepath=True)
      if txn_id_path:
        txn_id = long(txn_id_path.split(PATH_SEPARATOR)[-1].lstrip(
          APP_TX_PREFIX))
        if txn_id == 0:
          self.logger.warning("Created sequence ID 0 - deleting it.")
          self.run_with_retry(self.handle.delete, txn_id_path)
          txn_id_path = self.run_with_retry(self.handle.create, path, 
            value=str(value), acl=ZOO_ACL_OPEN, ephemeral=False, 
            sequence=True, makepath=True)
          return long(txn_id_path.split(PATH_SEPARATOR)[-1].lstrip(
            APP_TX_PREFIX))
        else:
          return txn_id
    except kazoo.exceptions.ZookeeperError as zoo_exception:
      self.logger.exception(zoo_exception)
      self.reestablish_connection()
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()
      
    raise ZKTransactionException("Unable to create sequence node with path" \
      " {0}, value {1}".format(path, value))

  def get_transaction_id(self, app_id, is_xg=False):
    """Acquires a new id for an upcoming transaction.

    Note that the caller must lock particular root entities using acquire_lock,
    and that the transaction ID expires after a constant amount of time.

    Args:
      app_id: A str representing the application we want to perform a
        transaction on.
      is_xg: A bool that indicates if this transaction operates across multiple
        entity groups.
    Returns:
      A long that represents the new transaction ID.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    timestamp = str(time.time())

    # First, make the ZK node for the actual transaction id.
    app_path = self.get_txn_path_before_getting_id(app_id)
    txn_id = self.create_sequence_node(app_path, timestamp)

    # Next, make the ZK node that indicates if this a XG transaction.
    if is_xg:
      xg_path = self.get_xg_path(app_id, txn_id)
      self.create_node(xg_path, timestamp)
    return txn_id

  def check_transaction(self, app_id, txid):
    """ Gets the status of the given transaction.

    Args:
      app_id: A str representing the application whose transaction we wish to
        query.
      txid: An int that indicates the transaction ID we should query.
    Returns:
      True if the transaction is in progress.
    Raises:
      ZKTransactionException: If the transaction is not in progress, or it
        has timed out.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    txpath = self.get_transaction_path(app_id, txid)
    try:
      if self.is_blacklisted(app_id, txid):
        raise ZKTransactionException("Transaction {0} timed out.".format(txid))
    except ZKInternalException as zk_exception:
      self.logger.exception(zk_exception)
      self.reestablish_connection()
      raise ZKTransactionException("Couldn't see if transaction {0} is valid" \
        .format(txid))

    try:
      if not self.run_with_retry(self.handle.exists, txpath):
        self.logger.debug(
          'check_transaction: {} does not exist'.format(txpath))
        raise ZKTransactionException('Transaction {} is invalid'.format(txid))
      return True
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()
      raise ZKTransactionException(
        'Unable to determine status of transaction {}'.format(txid))

  def is_in_transaction(self, app_id, txid, retries=5):
    """ Checks to see if the named transaction is currently running.

    Args:
      app_id: A str representing the application whose transaction we wish to
        query.
      txid: An int that indicates the transaction ID we should query.
    Returns:
      True if the transaction is in progress, and False otherwise.
    Raises:
      ZKTransactionException: If the transaction is blacklisted.
      ZKInternalException: If there was an error seeing if the transaction was
        blacklisted.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    tx_lock_path = self.get_transaction_lock_list_path(app_id, txid)
    if self.is_blacklisted(app_id, txid):
      raise ZKTransactionException(
        'Transaction {} is blacklisted'.format(txid))
    try:
      if not self.run_with_retry(self.handle.exists, tx_lock_path):
        return False
      return True
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      if retries > 0:
        self.logger.info(
          'Trying again to see if transaction {} is in progress. Retry #{}'
          .format(txid, retries))
        time.sleep(self.ZK_RETRY_TIME)
        return self.is_in_transaction(app_id=app_id, txid=txid,
          retries=retries - 1)
      self.reestablish_connection()
      raise ZKInternalException("Couldn't see if we are in transaction {0}" \
        .format(txid))

  def is_orphan_lock(self, tx_lockpath):
    """ Checks to see if a lock does not have a transaction linked.
   
    If the groomer misses to unlock a lock for whatever reason, we need
    to make sure the lock is eventually released.

    Args:
      tx_lockpath: A str, the path to the transaction using the lock.
    Returns:
      True if the lock is an orphan, and False otherwise.
    """
    try: 
      self.handle.get(tx_lockpath)
      return False
    except kazoo.exceptions.NoNodeError:
      return True

  def acquire_additional_lock(self, app_id, txid, entity_key, create):
    """ Acquire an additional lock for a cross group transaction.

    Args:
      app_id: A str representing the application ID.
      txid: The transaction ID you are acquiring a lock for. Built into
            the path.
      entity_key: Used to get the root path.
      create: A bool that indicates if we should create a new Zookeeper node
        to store the lock information in.
    Returns:
      Boolean, of true on success, false if lock can not be acquired.
    Raises:
      ZKTransactionException: If we can't acquire the lock for the given
        entity group, because a different transaction already has it.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    txpath = self.get_transaction_path(app_id, txid)
    lockrootpath = self.get_lock_root_path(app_id, entity_key)
    lockpath = None

    try:
      lockpath = self.run_with_retry(self.handle.create, lockrootpath,
        value=str(txpath), acl=ZOO_ACL_OPEN, ephemeral=False, 
        sequence=False, makepath=True)
    except kazoo.exceptions.NodeExistsError:
      # fail to get lock
      try:
        tx_lockpath = self.run_with_retry(self.handle.get, lockrootpath)[0]
        self.logger.error(
          'Lock {} in use by {}'.format(lockrootpath, tx_lockpath))
        if self.is_orphan_lock(tx_lockpath):
          self.logger.error(
            'Lock {} is an orphan lock. Releasing it'.format(lockrootpath))
          # Releasing the lock in question.
          self.handle.delete(lockrootpath)
          # Try to acquire the lock again.
          return self.acquire_additional_lock(app_id, txid, entity_key, create)
      except kazoo.exceptions.NoNodeError:
        # If the lock is released by another thread this can get tossed.
        # A race condition.
        self.logger.warning(
          'Lock {} was in use but was released'.format(lockrootpath))
      raise ZKTransactionException("acquire_additional_lock: There is " \
        "already another transaction using {0} lock".format(lockrootpath))
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()
      raise ZKTransactionException("Couldn't get a lock at path {0}" \
        .format(lockrootpath))

    transaction_lock_path = self.get_transaction_lock_list_path(app_id, txid)

    try:
      if create:
        self.run_with_retry(self.handle.create_async, transaction_lock_path,
          value=str(lockpath), acl=ZOO_ACL_OPEN, ephemeral=False,
          makepath=False, sequence=False)
      else:
        tx_lockpath = self.run_with_retry(self.handle.get,
          transaction_lock_path)[0]
        lock_list = tx_lockpath.split(LOCK_LIST_SEPARATOR)
        lock_list.append(lockpath)
        lock_list_str = LOCK_LIST_SEPARATOR.join(lock_list)
        self.run_with_retry(self.handle.set_async, transaction_lock_path,
          str(lock_list_str))
        self.logger.debug(
          'Set lock list path {} to value {}'
          .format(transaction_lock_path, lock_list_str))
        # We do this check last, otherwise we may have left over locks to 
        # to a lack of a lock path reference.
        if len(lock_list) > MAX_GROUPS_FOR_XG:
          raise ZKBadRequest("acquire_additional_lock: Too many " \
            "groups for this XG transaction.")

    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()
      raise ZKTransactionException("Couldn't create or set a lock at path {0}" \
        .format(transaction_lock_path))

    return True

  def is_xg(self, app_id, tx_id):
    """ Checks to see if the transaction can operate over multiple entity
      groups.

    Args:
      app_id: The application ID that the transaction operates over.
      tx_id: The transaction ID that may or may not be XG.
    Returns:
      True if the transaction is XG, False otherwise.
    Raises:
      ZKTransactionException: on ZooKeeper exceptions.
      ZKInternalException: If we can't tell if the transaction is a XG
        transaction or not.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    try:
      return self.run_with_retry(self.handle.exists, self.get_xg_path(app_id,
        tx_id))
    except kazoo.exceptions.ZookeeperError as zk_exception:
      raise ZKTransactionException("ZooKeeper exception:{0}"\
        .format(zk_exception)) 
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()
      raise ZKInternalException("Couldn't see if transaction {0} was XG " \
        "for app {1}".format(tx_id, app_id))

  def acquire_lock(self, app_id, txid, entity_key):
    """ Acquire lock for transaction. It will acquire additional locks
    if the transactions is XG.

    You must call get_transaction_id() first to obtain transaction ID.
    You could call this method anytime if the root entity key is same, 
    or different in the case of it being XG.

    Args:
      app_id: The application ID to acquire a lock for.
      txid: The transaction ID you are acquiring a lock for. Built into 
        the path. 
       entity_key: Used to get the root path.
    Returns:
      True on success, False otherwise.
    Raises:
      ZKTransactionException: If it could not get the lock.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    lockrootpath = self.get_lock_root_path(app_id, entity_key)

    try:
      if self.is_in_transaction(app_id, txid):  # use current lock
        transaction_lock_path = self.get_transaction_lock_list_path(
          app_id, txid)
        prelockpath = self.run_with_retry(self.handle.get,
          transaction_lock_path)[0]
        lock_list = prelockpath.split(LOCK_LIST_SEPARATOR)
        if lockrootpath in lock_list:
          return True
        else:
          if self.is_xg(app_id, txid):
            return self.acquire_additional_lock(app_id, txid, entity_key,
              create=False)
          else:
            raise ZKBadRequest("acquire_lock: You can not lock " \
              "different root entity in non-cross-group transaction.")
    except ZKInternalException as zk_exception:
      self.logger.exception(zk_exception)
      self.reestablish_connection()
      raise ZKTransactionException("An internal exception prevented us from " \
        "getting the lock for app id {0}, txid {1}, entity key {2}" \
        .format(app_id, txid, entity_key))
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()
      raise ZKTransactionException("Couldn't get lock for app id {0}, txid " \
        "{1}, entity key {2}".format(app_id, txid, entity_key))

    return self.acquire_additional_lock(app_id, txid, entity_key, create=True)

  def get_updated_key_list(self, app_id, txid):
    """ Gets a list of keys updated in this transaction.

    Args:
      app_id: A str corresponding to the application ID whose transaction we
        wish to query.
      txid: The transaction ID that we want to get a list of updated keys for.
    Returns:
      A list of (keys, txn_id) that have been updated in this transaction.
    Raises:
      ZKTransactionException: If the given transaction ID does not correspond
        to a transaction that is currently in progress.
    """
    txpath = self.get_transaction_path(app_id, txid)
    try:
      child_list = self.run_with_retry(self.handle.get_children, txpath)
      keylist = []
      for item in child_list:
        if re.match("^" + TX_UPDATEDKEY_PREFIX, item):
          keyandtx = self.run_with_retry(self.handle.get,
            PATH_SEPARATOR.join([txpath, item]))[0]
          key = urllib.unquote_plus(keyandtx.split(PATH_SEPARATOR)[0])
          txn_id = urllib.unquote_plus(keyandtx.split(PATH_SEPARATOR)[1])
          keylist.append((key, txn_id))
      return keylist
    except kazoo.exceptions.NoNodeError:
      raise ZKTransactionException("get_updated_key_list: Transaction ID {0} " \
        "is not valid.".format(txid))
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()
      raise ZKTransactionException("Couldn't get updated key list for appid " \
        "{0}, txid {1}".format(app_id, txid))

  def remove_tx_node(self, app_id, txid):
    """ Remove a transaction's sequence node.

    Args:
      app_id: A string specifying an application ID.
      txid: An integer specifying a transaction ID.
    """
    txpath = self.get_transaction_path(app_id, txid)
    try:
      self.run_with_retry(self.handle.delete, txpath, -1, True)
    except NoNodeError:
      return


  def release_lock(self, app_id, txid):
    """ Releases all locks acquired during this transaction.

    Callers must call acquire_lock before calling release_lock. Upon calling
    release_lock, the given transaction ID is no longer valid.

    Args:
      app_id: The application ID we are releasing a lock for.
      txid: The transaction ID we are releasing a lock for.
    Returns:
      True if the locks were released.
    Raises:
      ZKTransactionException: If any locks acquired during this transaction
        could not be released.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    self.check_transaction(app_id, txid)
    txpath = self.get_transaction_path(app_id, txid)
     
    transaction_lock_path = self.get_transaction_lock_list_path(app_id, txid)
    try:
      lock_list_str = self.run_with_retry(self.handle.get,
        transaction_lock_path)[0]
      lock_list = lock_list_str.split(LOCK_LIST_SEPARATOR)
      for lock_path in lock_list:
        self.run_with_retry(self.handle.delete, lock_path)
      self.run_with_retry(self.handle.delete, transaction_lock_path)
    except kazoo.exceptions.NoNodeError:
      try:
        if self.is_blacklisted(app_id, txid):
          raise ZKTransactionException(
            "Unable to release lock {0} for app id {1}" \
            .format(transaction_lock_path, app_id))
        else:
          return True
      except ZKInternalException as zk_exception:
        self.logger.exception(zk_exception)
        self.reestablish_connection()
        raise ZKTransactionException("Internal exception prevented us from " \
          "releasing lock {0} for app id {1}".format(transaction_lock_path,
          app_id))
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()
      raise ZKTransactionException("Couldn't release lock {0} for appid {1}" \
        .format(transaction_lock_path, app_id))

    try:
      if self.is_xg(app_id, txid):
        xg_path = self.get_xg_path(app_id, txid)
        self.run_with_retry(self.handle.delete, xg_path)

      for child in self.run_with_retry(self.handle.get_children, txpath):
        lock_path = PATH_SEPARATOR.join([txpath, child])
        self.logger.debug('Removing lock: {}'.format(lock_path))
        self.run_with_retry(self.handle.delete, lock_path)

      # This deletes the transaction root path.
      self.run_with_retry(self.handle.delete, txpath)

    except ZKInternalException as zk_exception:
      # Although there was a failure doing the async deletes, since we've
      # already released the locks above, we can safely return True here.
      self.logger.exception(zk_exception)
      self.reestablish_connection()
      return True
    except kazoo.exceptions.KazooException as kazoo_exception:
      # Although there was a failure doing the async deletes, since we've
      # already released the locks above, we can safely return True here.
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()
      return True

    return True

  def is_blacklisted(self, app_id, txid, retries=5):
    """ Checks to see if the given transaction ID has been blacklisted (that is,
    if it is no longer considered to be a valid transaction).

    Args:
      app_id: The application ID whose transaction ID we want to validate.
      txid: The transaction ID that we want to validate.
    Returns:
      True if the transaction is blacklisted, False otherwise.
    Raises:
      ZKInternalException: If we couldn't determine if the transaction was
        blacklisted or not.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    try:
      blacklist_root = self.get_blacklist_root_path(app_id)
      blacklist_txn = PATH_SEPARATOR.join([blacklist_root, 
        str(txid)]) 
      return self.run_with_retry(self.handle.exists, blacklist_txn)
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      if retries > 0:
        self.logger.info(
          'Trying again to see if transaction {} is blacklisted with retry #{}'
          .format(txid, retries))
        time.sleep(self.ZK_RETRY_TIME)
        return self.is_blacklisted(app_id=app_id, txid=txid,
                                   retries=retries - 1)
      self.reestablish_connection()
      raise ZKInternalException("Couldn't see if appid {0}'s transaction, " \
        "{1}, is blacklisted.".format(app_id, txid))


  def get_valid_transaction_id(self, app_id, target_txid, entity_key):
    """ This returns valid transaction id for the entity key.

    Args:
      app_id: A str representing the application ID.
      target_txid: The transaction id that we want to check for validness.
      entity_key: The entity that the transaction operates over.
    Returns:
      A long containing the latest valid transaction id, or zero if there is
      none.
    Raises:
      ZKInternalException: If we couldn't get a valid transaction ID.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    # If this is an ongoing transaction give the previous value.
    try:
      if self.is_in_transaction(app_id, target_txid):
        key_list = self.get_updated_key_list(app_id, target_txid)
        for (key, txn_id) in key_list:
          if entity_key == key:
            return long(txn_id)
    except ZKTransactionException, zk_exception:
      # If the transaction is blacklisted.
      # Get the valid id.
      vtxpath = self.get_valid_transaction_path(app_id, entity_key)
      try:
        return long(self.run_with_retry(self.handle.get, vtxpath)[0])
      except kazoo.exceptions.NoNodeError:
        # Blacklisted and without a valid ID.
        return long(0)
      except kazoo.exceptions.KazooException as kazoo_exception:
        self.logger.exception(kazoo_exception)
        self.reestablish_connection()
        raise ZKInternalException("Couldn't get valid transaction id for " \
          "app {0}, target txid {1}, entity key {2}".format(app_id, target_txid,
          entity_key))

    # The given target ID is not blacklisted or in an ongoing transaction.
    return target_txid

  def register_updated_key(self, app_id, current_txid, target_txid, entity_key):
    """ Registers a key which is a part of a transaction. This is to know
    what journal version we must rollback to upon failure.

    Args:
      app_id: A str representing the application ID.
      current_txid: The current transaction ID for which we'll rollback to upon 
        failure.
      target_txid: A long transaction ID we are rolling forward to.
      entity_key: A str key we are registering.
    Returns:
      True on success.
    Raises:
      ZKTransactionException: If the transaction is not valid.
      ZKInternalException: If we were unable to register the key.
    """
    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    vtxpath = self.get_valid_transaction_path(app_id, entity_key)

    try:
      if self.run_with_retry(self.handle.exists, vtxpath):
        # Update the transaction ID for entity if there is valid transaction.
        self.run_with_retry(self.handle.set_async, vtxpath, str(target_txid))
      else:
        # Store the updated key info into the current transaction node.
        value = PATH_SEPARATOR.join([urllib.quote_plus(entity_key),
          str(target_txid)])
        txpath = self.get_transaction_path(app_id, current_txid)

        if self.run_with_retry(self.handle.exists, txpath):
          self.handle.create_async(PATH_SEPARATOR.join([txpath,
            TX_UPDATEDKEY_PREFIX]), value=str(value), acl=ZOO_ACL_OPEN,
            ephemeral=False, sequence=True, makepath=False)
        else:
          raise ZKTransactionException("Transaction {0} is not valid.".format(
            current_txid))
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()
      raise ZKInternalException("Couldn't register updated key for app " \
        "{0}, current txid {1}, target txid {2}, entity_key {3}".format(app_id,
        current_txid, target_txid, entity_key))

    return True

  def notify_failed_transaction(self, app_id, txid):
    """ Marks the given transaction as failed, invalidating its use by future
    callers.

    This function also cleans up successful transactions that have expired.

    Args:
      app_id: The application ID whose transaction we wish to invalidate.
      txid: An int representing the transaction ID we wish to invalidate.
    Returns:
      True if the transaction was invalidated, False otherwise.
    """
    self.logger.debug('notify_failed_trasnsaction: app={}, txid={}'
                      .format(app_id, txid))

    lockpath = None
    lock_list = []

    if self.needs_connection or not self.handle.connected:
      self.reestablish_connection()

    txpath = self.get_transaction_path(app_id, txid)
    try:
      lockpath = self.run_with_retry(self.handle.get,
        PATH_SEPARATOR.join([txpath, TX_LOCK_PATH]))[0]
      lock_list = lockpath.split(LOCK_LIST_SEPARATOR)
    except kazoo.exceptions.NoNodeError:
      # There is no need to rollback because there is no lock.
      self.logger.debug('There is no lock for transaction {}'.format(txid))
      pass
    except kazoo.exceptions.ZookeeperError as zoo_exception:
      self.logger.exception(zoo_exception)
      return False
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()
      return False

    try:
      if lock_list:
        children = []
        try:
          children = self.run_with_retry(self.handle.get_children, txpath)
        except kazoo.exceptions.NoNodeError:
          pass

        # Copy valid transaction ID for each updated key into valid list.
        for child in children:
          if re.match("^" + TX_UPDATEDKEY_PREFIX, child):
            value = self.run_with_retry(self.handle.get,
              PATH_SEPARATOR.join([txpath, child]))[0]
            valuelist = value.split(PATH_SEPARATOR)
            key = urllib.unquote_plus(valuelist[0])
            vid = valuelist[1]
            vtxroot = self.get_valid_transaction_root_path(app_id)

            if not self.run_with_retry(self.handle.exists, vtxroot):
              self.run_with_retry(self.handle.create, vtxroot, DEFAULT_VAL,
                ZOO_ACL_OPEN, False, False, True)
            vtxpath = self.get_valid_transaction_path(app_id, key)
            self.run_with_retry(self.handle.create_async, vtxpath, str(vid),
              ZOO_ACL_OPEN)

      # Release the locks.
      for lock in lock_list:
        try:
          self.run_with_retry(self.handle.delete, lock)
        except kazoo.exceptions.NoNodeError:
          # Try to delete all nodes, so skip any failure to release a lock.
          pass  

      if self.is_xg(app_id, txid):
        try:
          self.run_with_retry(self.handle.delete, self.get_xg_path(app_id,
            txid))
        except kazoo.exceptions.NoNodeError:
          self.logger.error(
            'No node error when trying to remove {0}'.format(txid))

      # Remove the transaction paths.
      for item in self.run_with_retry(self.handle.get_children, txpath):
        try:
          self.run_with_retry(self.handle.delete,
            PATH_SEPARATOR.join([txpath, item]))
        except kazoo.exceptions.NoNodeError:
          self.logger.error(
            'No node error when trying to remove {}'.format(txid))

      self.logger.debug(
        'Notify failed transaction removing lock: {}'.format(txpath))
      self.run_with_retry(self.handle.delete, txpath)

    except ZKInternalException as zk_exception:
      self.logger.exception(zk_exception)
      return False
    except kazoo.exceptions.ZookeeperError as zk_exception:
      self.logger.exception(zk_exception)
      return False
    except kazoo.exceptions.KazooException as kazoo_exception:
      self.logger.exception(kazoo_exception)
      self.reestablish_connection()
      return False
      
    return True

  def reestablish_connection(self):
    """ Checks the connection and resets it as needed. """
    self.logger.warning('Re-establishing ZooKeeper connection.')
    try:
      self.handle.restart()
      self.needs_connection = False
      self.failure_count = 0
      self.logger.info('Restarted ZK connection successfully.')
      return
    except kazoo.exceptions.ZookeeperError:
      self.logger.exception(
        'Unable to restart ZK connection. Creating a new one.')
    except kazoo.exceptions.KazooException:
      self.logger.exception(
        'Unable to restart ZK connection. Creating a new one.')
    except Exception:
      self.logger.exception(
        'Unable to restart ZK connection. Creating a new one.')

    try:
      self.handle.stop()
    except kazoo.exceptions.ZookeeperError:
      self.logger.exception('Issue stopping ZK connection.')
    except kazoo.exceptions.KazooException:
      self.logger.exception('Issue stopping ZK connection.')
    except Exception:
      self.logger.exception('Issue stopping ZK connection.')

    try:
      self.handle.close()
    except kazoo.exceptions.ZookeeperError:
      self.logger.exception('Issue closing ZK connection.')
    except kazoo.exceptions.KazooException:
      self.logger.exception('Issue closing ZK connection.')
    except Exception:
      self.logger.exception('Issue closing ZK connection.')

    self.logger.warning('Creating a new connection to ZK')
    reconnect_error = False

    self.handle = kazoo.client.KazooClient(hosts=self.host,
      max_retries=self.DEFAULT_NUM_RETRIES, timeout=self.DEFAULT_ZK_TIMEOUT)

    try:
      self.handle.start()
    except kazoo.exceptions.KazooException as kazoo_exception:
      reconnect_error = True
      self.logger.exception(kazoo_exception)
    except Exception as exception:
      reconnect_error = True
      self.logger.exception(exception)

    if reconnect_error:
      self.logger.error('Error re-establishing ZooKeeper connection!')
      self.needs_connection = True
      self.failure_count += 1
    else:
      self.logger.info('Successfully created a new connection')
      self.needs_connection = False
      self.failure_count = 0

    if self.failure_count > self.MAX_CONNECTION_FAILURES:
      self.logger.critical('Too many connection errors to ZooKeeper. Aborting')
      sys.exit(1)

  def gc_runner(self):
    """ Transaction ID garbage collection (GC) runner.

    Note: This must be running as separate thread.
    """
    self.logger.debug('Starting GC thread.')

    while self.gc_running:
      # Scan each application's last GC time.
      try:
        app_list = self.run_with_retry(self.handle.get_children, APPS_PATH)

        for app in app_list:
          app_id = urllib.unquote_plus(app)
          # App is already encoded, so we should not use
          # self.get_app_root_path.
          app_path = PATH_SEPARATOR.join([APPS_PATH, app])
          self.try_garbage_collection(app_id, app_path)
      except kazoo.exceptions.NoNodeError:
        # There were no nodes for this application.
        pass
      except kazoo.exceptions.OperationTimeoutError:
        self.logger.exception('GC timeout when fetching {}'.format(APPS_PATH))
      except (ZookeeperError, KazooException):
        self.logger.exception('Error when trying garbage collection')
        self.reestablish_connection()
      except Exception:
        self.logger.exception('Unknown exception')
        self.reestablish_connection()

      with self.gc_cv:
        self.gc_cv.wait(GC_INTERVAL)

    self.logger.debug('Stopping GC thread.')

  def try_garbage_collection(self, app_id, app_path):
    """ Try to garbage collect timed out transactions.
  
    Args:
      app_id: The application ID.
      app_path: The application path for which we're garbage collecting.
    Returns:
      True if the garbage collector ran, False otherwise.
    """
    last_time = 0
    gc_time_path = PATH_SEPARATOR.join([app_path, GC_TIME_PATH])
    try:
      val = self.run_with_retry(self.handle.get, gc_time_path)[0]
      last_time = float(val)
    except kazoo.exceptions.NoNodeError:
      last_time = 0
    except (ZookeeperError, KazooException):
      self.logger.exception('Error when fetching {}'.format(gc_time_path))
      self.reestablish_connection()
      return False
    except Exception:
      self.logger.exception('Unknown exception')
      self.reestablish_connection()
      return False
 
    # If the last time plus our GC interval is less than the current time,
    # that means its time to run the GC again.
    if last_time + GC_INTERVAL < time.time():
      gc_path = PATH_SEPARATOR.join([app_path, GC_LOCK_PATH])
      try:
        now = str(time.time())
        # Get the global GC lock.
        self.run_with_retry(self.handle.create, gc_path, value=now, 
          acl=ZOO_ACL_OPEN, ephemeral=True)
        try:
          self.execute_garbage_collection(app_id, app_path)
          # Update the last time when the GC was successful.
          now = str(time.time())
          self.update_node(PATH_SEPARATOR.join([app_path, GC_TIME_PATH]), now)
        except Exception as exception:
          self.logger.exception(exception)
        # Release the lock.
        self.run_with_retry(self.handle.delete, gc_path)
      except kazoo.exceptions.NodeExistsError:
        # Failed to obtain the GC lock. Try again later.
        pass
      except (ZookeeperError, KazooException):
        self.logger.exception('Error while executing garbage collection')
        self.reestablish_connection()
        return False
      except Exception:
        self.logger.exception('Unknown exception')
        self.reestablish_connection()
        return False
 
      return True
    return False

  def get_lock_with_path(self, path):
    """ Tries to get the lock based on path.

    Args:
      path: A str, the lock path.
    Returns:
      True if the lock was obtained, False otherwise.
    """
    try:
      now = str(time.time())
      self.run_with_retry(self.handle.create, path, value=now,
        acl=ZOO_ACL_OPEN, ephemeral=True)
    except kazoo.exceptions.NoNodeError:
      self.logger.error('Unable to create {}'.format(path))
      return False
    except kazoo.exceptions.NodeExistsError:
      return False
    except (kazoo.exceptions.SystemZookeeperError, ZookeeperError,
            KazooException, SystemError):
      self.logger.exception('Unable to create {}'.format(path))
      self.reestablish_connection()
      return False
    except Exception:
      self.logger.exception('Unknown exception')
      self.reestablish_connection()
      return False
      
    return True

  def release_lock_with_path(self, path):
    """ Releases lock based on path.
   
    Args:
      path: A str, the lock path.
    Returns:
      True on success, False on system failures.
    Raises:
      ZKTransactionException: If the lock could not be released.
    """
    try:
      self.run_with_retry(self.handle.delete, path)
    except kazoo.exceptions.NoNodeError:
      raise ZKTransactionException('Unable to delete lock: {0}'.format(path))
    except (kazoo.exceptions.SystemZookeeperError, KazooException,
            SystemError):
      self.logger.exception('Unable to delete lock: {0}'.format(path))
      self.reestablish_connection()
      return False
    except Exception:
      self.logger.exception('Unknown exception')
      self.reestablish_connection()
      return False
    return True

  def establish_batch_lock(self, app, transaction, status_exists):
    """ Ensure that no concurrent processes continue to work on the batch. The
    datastore uses a lightweight transaction when marking the batch as applied.
    So if this modifies it first, the datastore batch will fail.

    Args:
      app: A string containing the application ID.
      transaction: An integer containing the transaction ID.
      status_exists: A boolean indicating the presence of a batch status row.
    """
    parameters = {'app': app, 'transaction': transaction}
    if status_exists:
      clear_status = """
        DELETE FROM batch_status
        WHERE app = %(app)s AND transaction = %(transaction)s
        IF applied = False
      """
      result = self.db_access.session.execute(clear_status, parameters)
      if not result.was_applied:
        raise BatchInProgress('The batch for {}:{} is still in progress'.
                              format(app, transaction))
      return

    insert_marker = """
      INSERT INTO batch_status (app, transaction, applied)
      VALUES (%(app)s, %(transaction)s, False)
      IF NOT EXISTS
    """
    result = self.db_access.session.execute(insert_marker, parameters)
    if not result.was_applied:
      # Another process is already working on the batch.
      raise BatchInProgress(
        'The batch for {} is still in progress'.format(transaction))

  def clean_up_batch(self, app, transaction):
    """ Clean up temporary batch data.

    Args:
      app: A string containing the application ID.
      transaction: An integer containing the transaction ID.
    """
    self.logger.debug(
      'Cleaning up batch: app={}, transaction={}'.format(app, transaction))
    parameters = {'app': app, 'transaction': transaction}
    clear_batch = """
      DELETE FROM batches
      WHERE app = %(app)s AND transaction = %(transaction)s
    """
    self.db_access.session.execute(clear_batch, parameters)
    clear_status = """
      DELETE FROM batch_status
      WHERE app = %(app)s AND transaction = %(transaction)s
    """
    self.db_access.session.execute(clear_status, parameters)

  def resolve_batch(self, app, transaction):
    """ Check if batch completed and apply mutations if necessary.

    Args:
      app: A string containing the application ID.
      transaction: An integer containing the transaction ID.
    """
    session = self.db_access.session
    parameters = {'app': app, 'transaction': transaction}
    select_applied = """
      SELECT applied FROM batch_status
      WHERE app = %(app)s AND transaction = %(transaction)s
    """
    try:
      applied = session.execute(select_applied, parameters)[0].applied
    except IndexError:
      # If there's no status entry for this batch, clean up.
      self.establish_batch_lock(app, transaction, status_exists=False)
      self.clean_up_batch(app, transaction)
      return

    if not applied:
      self.establish_batch_lock(app, transaction, status_exists=True)
      self.clean_up_batch(app, transaction)
      return

    composite_indices = [entity_pb.CompositeIndex(index)
                         for index in self.db_access.get_indices(str(app))]

    self.logger.debug(
      'Applying batch: app={}, transaction={}'.format(app, transaction))
    select_mutations = """
      SELECT old_value, new_value FROM batches
      WHERE app = %(app)s AND transaction = %(transaction)s
    """
    results = session.execute(select_mutations, parameters)
    for result in results:
      old_entity = result.old_value
      if old_entity is not None:
        old_entity = entity_pb.EntityProto(old_entity)

      new_entity = result.new_value
      if new_entity is not None:
        new_entity = entity_pb.EntityProto(new_entity)

      if new_entity is None:
        mutations = cassandra_interface.deletions_for_entity(
          old_entity, composite_indices)
      else:
        mutations = cassandra_interface.mutations_for_entity(
          new_entity, transaction, old_entity, composite_indices)
      self.db_access.apply_mutations(mutations)

    self.clean_up_batch(app, transaction)

  def execute_garbage_collection(self, app_id, app_path):
    """ Execute garbage collection for an application.
    
    Args:
      app_id: The application ID.
      app_path: The application path. 
    """
    start = time.time()
    # Get the transaction ID list.
    txrootpath = PATH_SEPARATOR.join([app_path, APP_TX_PATH])
    try:
      txlist = self.run_with_retry(self.handle.get_children, txrootpath)
    except kazoo.exceptions.NoNodeError:
      # there is no transaction yet.
      return
    except (ZookeeperError, KazooException):
      self.logger.exception('Unable to get children of {}'.format(txrootpath))
      self.reestablish_connection()
      return
    except Exception:
      self.logger.exception('Unknown exception')
      self.reestablish_connection()
      return
    # Verify the time stamp of each transaction.
    for txid in txlist:
      if not re.match("^" + APP_TX_PREFIX + '\d', txid):
        self.logger.debug(
          'Skipping {} because it is not a transaction'.format(txid))
        continue

      txpath = PATH_SEPARATOR.join([txrootpath, txid])

      try:
        txtime = float(self.run_with_retry(self.handle.get, txpath)[0])
        # If the timeout plus our current time is in the future, then
        # we have not timed out yet.
        if txtime + MAX_TX_DURATION < time.time():
          transaction = long(txid.lstrip(APP_TX_PREFIX))
          try:
            self.resolve_batch(app_id, transaction)
            self.notify_failed_transaction(app_id, transaction)
          except BatchInProgress:
            self.logger.exception(
              'Failed to clean up lock for {}:{}'.format(app_id, transaction))
      except kazoo.exceptions.NoNodeError:
        # Transaction id disappeared during garbage collection.
        # The transaction may have finished successfully.
        pass
      except (ZookeeperError, KazooException):
        self.logger.exception(
          'Error while running GC for {}:{}'.format(app_id, txid))
        self.reestablish_connection()
        return
      except Exception:
        self.logger.exception('Unknown exception')
        self.reestablish_connection()
        return 
    self.logger.debug('Lock GC took {} seconds.'.format(time.time() - start))

  def get_current_transactions(self, project):
    """ Fetch a list of open transactions for a given project.

    Args:
      project: A string containing a project ID.
    Returns:
      A list of integers specifying transaction IDs.
    """
    project_path = PATH_SEPARATOR.join([APPS_PATH, project])
    txrootpath = PATH_SEPARATOR.join([project_path, APP_TX_PATH])
    try:
      txlist = self.run_with_retry(self.handle.get_children, txrootpath)
    except kazoo.exceptions.NoNodeError:
      # there is no transaction yet.
      return []
    return [int(txid.lstrip(APP_TX_PREFIX)) for txid in txlist]
