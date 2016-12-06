#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
AppScale modifications 

Distributed Method:
All calls are made to a datastore server for queries, gets, puts, and deletes,
index functions, transaction functions.
"""

import collections
import datetime
import logging
import os
import time
import sys
import threading
import warnings

from google.appengine.api import api_base_pb
from google.appengine.api import apiproxy_stub
from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore
from google.appengine.api import datastore_errors
from google.appengine.api import datastore_types
from google.appengine.api import users
from google.appengine.datastore import datastore_pb
from google.appengine.datastore import datastore_index
from google.appengine.runtime import apiproxy_errors
from google.net.proto import ProtocolBuffer
from google.appengine.datastore import entity_pb
from google.appengine.ext.remote_api import remote_api_pb
from google.appengine.datastore import old_datastore_stub_util

# Where the SSL certificate is placed for encrypted communication.
CERT_LOCATION = "/etc/appscale/certs/mycert.pem"

# Where the SSL private key is placed for encrypted communication.
KEY_LOCATION = "/etc/appscale/certs/mykey.pem"

# The default SSL port to connect to.
SSL_DEFAULT_PORT = 8443

try:
  __import__('google.appengine.api.taskqueue.taskqueue_service_pb')
  taskqueue_service_pb = sys.modules.get(
      'google.appengine.api.taskqueue.taskqueue_service_pb')
except ImportError:
  from google.appengine.api.taskqueue import taskqueue_service_pb

warnings.filterwarnings('ignore', 'tempnam is a potential security risk')


entity_pb.Reference.__hash__ = lambda self: hash(self.Encode())
datastore_pb.Query.__hash__ = lambda self: hash(self.Encode())
datastore_pb.Transaction.__hash__ = lambda self: hash(self.Encode())


_MAX_QUERY_COMPONENTS = 100


_BATCH_SIZE = 20


_MAX_ACTIONS_PER_TXN = 5


_MAX_INT_32 = 2**31-1

class InternalCursor():
  """ Keeps track of where we are in a query. Used for when queries are done
  in batches.
  """
  def __init__(self, query, last_cursor, offset):
    """ Constructor.

    Args:
      query: Starting query, a datastore_pb.Query.
      last_cursor: A compiled cursor, the last from a result list.
      offset: The number of entities we've seen so far.
    """
    # Count is the limit we want to hit so we know we're done.
    self.__count = _MAX_INT_32
    if query.has_count():
      self.__count = query.count()
    elif query.has_limit():
      self.__count = query.limit()
    self.__query = query
    self.__last_cursor = last_cursor
    self.__creation = time.time()
    # Lets us know how many results we've seen so far. When
    # this hits the count we know we're done.
    self.__offset = offset

  def get_query(self):
    return self.__query

  def get_count(self):
    return self.__count

  def get_last_cursor(self):
    return self.__last_cursor

  def get_offset(self):
    return self.__offset

  def get_timestamp(self):
    return self.__creation

  def set_last_cursor(self, last_cursor):
    self.__last_cursor = last_cursor

  def set_offset(self, offset):
    self.__offset = offset

class DatastoreDistributed(apiproxy_stub.APIProxyStub):
  """ A central server hooks up to a db and communicates via protocol 
      buffers.

  """

  _PROPERTY_TYPE_TAGS = {
    datastore_types.Blob: entity_pb.PropertyValue.kstringValue,
    bool: entity_pb.PropertyValue.kbooleanValue,
    datastore_types.Category: entity_pb.PropertyValue.kstringValue,
    datetime.datetime: entity_pb.PropertyValue.kint64Value,
    datastore_types.Email: entity_pb.PropertyValue.kstringValue,
    float: entity_pb.PropertyValue.kdoubleValue,
    datastore_types.GeoPt: entity_pb.PropertyValue.kPointValueGroup,
    datastore_types.IM: entity_pb.PropertyValue.kstringValue,
    int: entity_pb.PropertyValue.kint64Value,
    datastore_types.Key: entity_pb.PropertyValue.kReferenceValueGroup,
    datastore_types.Link: entity_pb.PropertyValue.kstringValue,
    long: entity_pb.PropertyValue.kint64Value,
    datastore_types.PhoneNumber: entity_pb.PropertyValue.kstringValue,
    datastore_types.PostalAddress: entity_pb.PropertyValue.kstringValue,
    datastore_types.Rating: entity_pb.PropertyValue.kint64Value,
    str: entity_pb.PropertyValue.kstringValue,
    datastore_types.Text: entity_pb.PropertyValue.kstringValue,
    type(None): 0,
    unicode: entity_pb.PropertyValue.kstringValue,
    users.User: entity_pb.PropertyValue.kUserValueGroup,
    }

  def __init__(self,
               app_id,
               datastore_location,
               history_file=None,
               require_indexes=False,
               service_name='datastore_v3',
               trusted=False,
               root_path='/var/apps/'):
    """Constructor.

    Args:
      app_id: string
      datastore_location: location of datastore server
      history_file: DEPRECATED. No-op.
      require_indexes: bool, default False.  If True, composite indexes must
          exist in index.yaml for queries that need them.
      service_name: Service name expected for all calls.
      trusted: bool, default False.  If True, this stub allows an app to
        access the data of another app.
      root_path: A str, the path where index.yaml can be found.
    """
    super(DatastoreDistributed, self).__init__(service_name)

    # TODO lock any use of these global variables
    assert isinstance(app_id, basestring) and app_id != ''
    self.__app_id = app_id
    self.__datastore_location = datastore_location
    self.__index_cache = {}
    self.__is_encrypted = True
    res = self.__datastore_location.split(':')
    if len(res) == 2:
      if int(res[1]) != SSL_DEFAULT_PORT:
        self.__is_encrypted = False

    self.SetTrusted(trusted)

    self.__queries = {}

    self.__tx_actions = {}

    self.__cursor_id = 1
    self.__cursor_lock = threading.Lock()

    self.__require_indexes = require_indexes
    self.__root_path = root_path + self.__app_id + "/app"
    self.__cached_yaml = (None, None, None)
    if require_indexes:
      self._SetupIndexes()

  def __getCursorID(self):
    """ Gets a cursor identifier. """
    self.__cursor_lock.acquire()
    self.__cursor_id += 1
    cursor_id = self.__cursor_id
    self.__cursor_lock.release()
    return cursor_id 

  def Clear(self):
    """ Clears the datastore by deleting all currently stored entities and
    queries. """
    pass

  def SetTrusted(self, trusted):
    """Set/clear the trusted bit in the stub.

    This bit indicates that the app calling the stub is trusted. A
    trusted app can write to datastores of other apps.

    Args:
      trusted: boolean.
    """
    self.__trusted = trusted

  def __ValidateAppId(self, app_id):
    """Verify that this is the stub for app_id.

    Args:
      app_id: An application ID.

    Raises:
      datastore_errors.BadRequestError: if this is not the stub for app_id.
    """
    assert app_id
    if not self.__trusted and app_id != self.__app_id:
      raise datastore_errors.BadRequestError(
          'app %s cannot access app %s\'s data' % (self.__app_id, app_id))

  def __ValidateKey(self, key):
    """Validate this key.

    Args:
      key: entity_pb.Reference

    Raises:
      datastore_errors.BadRequestError: if the key is invalid
    """
    assert isinstance(key, entity_pb.Reference)

    self.__ValidateAppId(key.app())

    for elem in key.path().element_list():
      if elem.has_id() == elem.has_name():
        raise datastore_errors.BadRequestError(
          'each key path element should have id or name but not both: %r' % key)

  def _AppIdNamespaceKindForKey(self, key):
    """ Get (app, kind) tuple from given key.

    The (app, kind) tuple is used as an index into several internal
    dictionaries, e.g. __entities.

    Args:
      key: entity_pb.Reference

    Returns:
      Tuple (app, kind), both are unicode strings.
    """
    last_path = key.path().element_list()[-1]
    return (datastore_types.EncodeAppIdNamespace(key.app(), key.name_space()),
        last_path.type())

  READ_PB_EXCEPTIONS = (ProtocolBuffer.ProtocolBufferDecodeError, LookupError,
                        TypeError, ValueError)
  READ_ERROR_MSG = ('Data in %s is corrupt or a different version. '
                    'Try running with the --clear_datastore flag.\n%r')
  READ_PY250_MSG = ('Are you using FloatProperty and/or GeoPtProperty? '
                    'Unfortunately loading float values from the datastore '
                    'file does not work with Python 2.5.0. '
                    'Please upgrade to a newer Python 2.5 release or use '
                    'the --clear_datastore flag.\n')

  def Read(self):
    """ Does Nothing    """
    return

  def Write(self):
    """ Does Nothing   """
    return 

  def Flush(self):
    """ Does Nothing  """
    return

  def MakeSyncCall(self, service, call, request, response, request_id=None):
    """ The main RPC entry point. service must be 'datastore_v3'.
    """
    self.assertPbIsInitialized(request)
    super(DatastoreDistributed, self).MakeSyncCall(service,
                                                call,
                                                request,
                                                response,
                                                request_id)
    self.assertPbIsInitialized(response)

  def assertPbIsInitialized(self, pb):
    """Raises an exception if the given PB is not initialized and valid."""
    explanation = []
    assert pb.IsInitialized(explanation), explanation
    pb.Encode()

  def QueryHistory(self):
    """Returns a dict that maps Query PBs to times they've been run."""
    return []

  def _maybeSetDefaultAuthDomain(self):
    """ Sets default auth domain if not set. """
    auth_domain = os.environ.get("AUTH_DOMAIN")
    if not auth_domain:
      os.environ['AUTH_DOMAIN'] = "appscale.com"

  def _RemoteSend(self, request, response, method):
    """Sends a request remotely to the datstore server. """
    tag = self.__app_id
    self._maybeSetDefaultAuthDomain() 
    user = users.GetCurrentUser()
    if user != None:
      tag += ":" + user.email()
      tag += ":" + user.nickname()
      tag += ":" + user.auth_domain()
    api_request = remote_api_pb.Request()
    api_request.set_method(method)
    api_request.set_service_name("datastore_v3")
    api_request.set_request(request.Encode())

    api_response = remote_api_pb.Response()
    api_response = api_request.sendCommand(self.__datastore_location,
      tag,
      api_response,
      1,
      self.__is_encrypted, 
      KEY_LOCATION,
      CERT_LOCATION)

    if not api_response or not api_response.has_response():
      raise datastore_errors.InternalError(
          'No response from db server on %s requests.' % method)
    
    if api_response.has_application_error():
      error_pb = api_response.application_error()
      logging.error(error_pb.detail())
      raise apiproxy_errors.ApplicationError(error_pb.code(),
                                             error_pb.detail())

    if api_response.has_exception():
      raise api_response.exception()
   
    response.ParseFromString(api_response.response())

  def _Dynamic_Put(self, put_request, put_response):
    """Send a put request to the datastore server. """
    put_request.set_trusted(self.__trusted)
    
    ent_kinds = []
    for ent in put_request.entity_list():
      last_path = ent.key().path().element_list()[-1]
      if last_path.type() not in ent_kinds:
        ent_kinds.append(last_path.type())

    for kind in ent_kinds:
      indexes = self.__index_cache.get(kind)
      if indexes:
        for index in indexes:
          new_composite = put_request.add_composite_index()
          new_composite.CopyFrom(index)

    self._RemoteSend(put_request, put_response, "Put")
    return put_response 

  def _Dynamic_Get(self, get_request, get_response):
    """Send a get request to the datastore server. """
    self._RemoteSend(get_request, get_response, "Get")
    return get_response


  def _Dynamic_Delete(self, delete_request, delete_response):
    """Send a delete request to the datastore server. 
  
    Args:
      delete_request: datastore_pb.DeleteRequest.
      delete_response: datastore_pb.DeleteResponse.
    Returns:
      A datastore_pb.DeleteResponse from the AppScale datastore server.
    """
    # Determine if there are composite indexes that need to be deleted.
    # The datastore service will look up meta data to figure out which
    # composite indexes apply.
    ent_kinds = []
    for key in delete_request.key_list():
      last_path = key.path().element_list()[-1]
      if last_path.type() not in ent_kinds:
        ent_kinds.append(last_path.type())
 
    has_composites = False
    for kind in ent_kinds:
      indexes = self.__index_cache.get(kind)
      if indexes:
        has_composites = True
        break

    if has_composites:
      delete_request.set_mark_changes(True)

    delete_request.set_trusted(self.__trusted)
    self._RemoteSend(delete_request, delete_response, "Delete")
    return delete_response

  def _Dynamic_RunQuery(self, query, query_result):
    """Send a query request to the datastore server. """
    if query.has_transaction():
      if not query.has_ancestor():
        raise apiproxy_errors.ApplicationError(
          datastore_pb.Error.BAD_REQUEST,
          'Only ancestor queries are allowed inside transactions.')
    (filters, orders) = datastore_index.Normalize(query.filter_list(),
                                                  query.order_list(), [])
    
    old_datastore_stub_util.FillUsersInQuery(filters)

    if not query.has_app():
      query.set_app(self.__app_id)
    self.__ValidateAppId(query.app())

    # Set the composite index if it applies.
    indexes = []
    if query.has_kind():
      kind_indexes = self.__index_cache.get(query.kind())
      if kind_indexes:
        indexes.extend(kind_indexes)
   
    index_to_use = _FindIndexToUse(query, indexes)
    if index_to_use != None:
      new_index = query.add_composite_index()
      new_index.MergeFrom(index_to_use)

    self._RemoteSend(query, query_result, "RunQuery")
    results = query_result.result_list()
    for result in results:
      old_datastore_stub_util.PrepareSpecialPropertiesForLoad(result)

    last_cursor = None
    if query_result.has_compiled_cursor():
      last_cursor = query_result.compiled_cursor()

    if query_result.more_results():
      new_cursor = InternalCursor(query, last_cursor, len(results))
      cursor_id = self.__getCursorID()
      cursor = query_result.mutable_cursor()
      cursor.set_app(self.__app_id)
      cursor.set_cursor(cursor_id)
      self.__queries[cursor_id] = new_cursor

    if query.compile():
      compiled_query = query_result.mutable_compiled_query()
      compiled_query.set_keys_only(query.keys_only())
      compiled_query.mutable_primaryscan().set_index_name(query.Encode())

  def _Dynamic_Next(self, next_request, query_result):
    """Get the next set of entities from a previously run query. """
    self.__ValidateAppId(next_request.cursor().app())

    cursor_handle = next_request.cursor().cursor()
    if cursor_handle not in self.__queries:
      raise apiproxy_errors.ApplicationError(
            datastore_pb.Error.BAD_REQUEST, 
            'Cursor %d not found' % cursor_handle)
 
    internal_cursor = self.__queries.get(cursor_handle)
    last_cursor = internal_cursor.get_last_cursor()
    query = internal_cursor.get_query()

    if not last_cursor:
      query_result.set_more_results(False)
      if next_request.compile():
        compiled_query = query_result.mutable_compiled_query()
        compiled_query.set_keys_only(query.keys_only())
        compiled_query.mutable_primaryscan().set_index_name(query.Encode())
      del self.__queries[cursor_handle]
      return

    if internal_cursor.get_offset() >= internal_cursor.get_count():
      query_result.set_more_results(False)
      query_result.mutable_compiled_cursor().CopyFrom(last_cursor)
      if next_request.compile():
        compiled_query = query_result.mutable_compiled_query()
        compiled_query.set_keys_only(query.keys_only())
        compiled_query.mutable_primaryscan().set_index_name(query.Encode())
      del self.__queries[cursor_handle]
      return
 
    count = _BATCH_SIZE
    if next_request.has_count():
      count = next_request.count()

    query.set_count(count)
    if next_request.has_offset():
      query.set_offset(next_request.offset())
    if next_request.has_compile():
      query.set_compile(next_request.compile())

    # Remove any offset since first RunQuery deals with it.
    query.clear_offset()

    query.mutable_compiled_cursor().CopyFrom(last_cursor)

    self._RemoteSend(query, query_result, "RunQuery")
    results = query_result.result_list()
    for result in results:
      old_datastore_stub_util.PrepareSpecialPropertiesForLoad(result)

    if len(results) > 0:
      if query_result.has_compiled_cursor():
        last_cursor = query_result.compiled_cursor()
        internal_cursor.set_last_cursor(last_cursor)
      offset = internal_cursor.get_offset()
      internal_cursor.set_offset(offset + len(results))
      query_result.set_more_results(internal_cursor.get_offset() < \
        internal_cursor.get_count())
    else:
      query_result.mutable_compiled_cursor().CopyFrom(last_cursor)
      query_result.set_more_results(False)
  
    if query.compile():
      compiled_query = query_result.mutable_compiled_query()
      compiled_query.set_keys_only(query.keys_only())
      compiled_query.mutable_primaryscan().set_index_name(query.Encode())
   
    if not query_result.more_results():
      del self.__queries[cursor_handle]
    else:
      cursor = query_result.mutable_cursor()                                    
      cursor.set_app(self.__app_id)                                                  
      cursor.set_cursor(cursor_handle)

  def _Dynamic_Count(self, query, integer64proto):
    """Get the number of entities for a query. """
    query_result = datastore_pb.QueryResult()
    self._Dynamic_RunQuery(query, query_result)
    count = query_result.result_size()
    integer64proto.set_value(count)

  def _Dynamic_BeginTransaction(self, request, transaction):
    """Send a begin transaction request from the datastore server. """
    request.set_app(self.__app_id)
    self._RemoteSend(request, transaction, "BeginTransaction")
    self.__tx_actions[transaction.handle()] = []
    return transaction

  def _Dynamic_AddActions(self, request, _):
    """Associates the creation of one or more tasks with a transaction.

    Args:
      request: A taskqueue_service_pb.TaskQueueBulkAddRequest containing the
          tasks that should be created when the transaction is comitted.
    """
    transaction = request.add_request_list()[0].transaction()
    txn_actions = self.__tx_actions[transaction.handle()]
    if ((len(txn_actions) + request.add_request_size()) >
        _MAX_ACTIONS_PER_TXN):
      raise apiproxy_errors.ApplicationError(
          datastore_pb.Error.BAD_REQUEST,
          'Too many messages, maximum allowed %s' % _MAX_ACTIONS_PER_TXN)

    new_actions = []
    for add_request in request.add_request_list():
      clone = taskqueue_service_pb.TaskQueueAddRequest()
      clone.CopyFrom(add_request)
      clone.clear_transaction()
      new_actions.append(clone)

    txn_actions.extend(new_actions)


  def _Dynamic_Commit(self, transaction, transaction_response):
    """ Send a transaction request to commit a transaction to the 
        datastore server. """
    transaction.set_app(self.__app_id)

    self._RemoteSend(transaction, transaction_response, "Commit")

    response = taskqueue_service_pb.TaskQueueAddResponse()
    try:
      for action in self.__tx_actions[transaction.handle()]:
        try:
          apiproxy_stub_map.MakeSyncCall(
              'taskqueue', 'Add', action, response)
        except apiproxy_errors.ApplicationError, e:
          logging.warning('Transactional task %s has been dropped, %s',
                          action, e)

    finally:
      try:
        del self.__tx_actions[transaction.handle()]
      except KeyError:
        pass
   
  def _Dynamic_Rollback(self, transaction, transaction_response):
    """ Send a rollback request to the datastore server. """
    transaction.set_app(self.__app_id)

    try:
      del self.__tx_actions[transaction.handle()]
    except KeyError:
      pass

    self._RemoteSend(transaction, transaction_response, "Rollback")
 
    return transaction_response

  def _Dynamic_GetSchema(self, req, schema):
    """ Get the schema of a particular kind of entity. """
    app_str = req.app()
    self.__ValidateAppId(app_str)
    schema.set_more_results(False)

  def _Dynamic_AllocateIds(self, allocate_ids_request, allocate_ids_response):
    """Send a request for allocation of IDs to the datastore server. """
    self._RemoteSend(allocate_ids_request, allocate_ids_response, "AllocateIds")
    return  allocate_ids_response

  def _Dynamic_CreateIndex(self, index, id_response):
    """ Create a new index. Currently stubbed out."""
    if index.id() != 0:
      raise apiproxy_errors.ApplicationError(datastore_pb.Error.BAD_REQUEST,
                                             'New index id must be 0.')
    self._RemoteSend(index, id_response, "CreateIndex")
    return id_response

  def _Dynamic_GetIndices(self, app_str, composite_indices):
    """ Gets the indices of the current app.

    Args:
      app_str: A api_base_pb.StringProto, the application identifier.
      composite_indices: datastore_pb.CompositeIndices protocol buffer.

    Returns:
      A datastore_pb.CompositesIndices containing the current indexes 
      used by this application.
    """
    self._RemoteSend(app_str, composite_indices, "GetIndices")
    return composite_indices

  def _Dynamic_UpdateIndex(self, index, void):
    """ Updates the indices of the current app. Tells the AppScale datastore
      server to build out the new index with existing data.

    Args:
      index: A datastore_pb.CompositeIndex, the composite index to update.
      void: A entity_pb.VoidProto.
    """
    self._RemoteSend(index, void, "UpdateIndex")
    return 
    
  def _Dynamic_DeleteIndex(self, index, void):
    """ Deletes an index of the current app.

    Args:
      index: A entity_pb.CompositeIndex, the composite index to delete.
      void: A entity_pb.VoidProto.
    Returns:
      A entity_pb.VoidProto. 
    """
    self._RemoteSend(index, void, "DeleteIndex")
    return void

  def _SetupIndexes(self, _open=open):
    """Ensure that the set of existing composite indexes matches index.yaml.
    
    Create any new indexes, and delete indexes which are no longer required.
   
    Args:
      _open: Function used to open a file.
    """
    if not self.__root_path:
      logging.warning("No index.yaml was loaded.")
      return
    index_yaml_file = os.path.join(self.__root_path, 'index.yaml')
    if (self.__cached_yaml[0] == index_yaml_file and
        os.path.exists(index_yaml_file) and
        os.path.getmtime(index_yaml_file) == self.__cached_yaml[1]):
      requested_indexes = self.__cached_yaml[2]
    else:
      try:
        index_yaml_mtime = os.path.getmtime(index_yaml_file)
        fh = _open(index_yaml_file, 'r')
      except (OSError, IOError):
        logging.info("Error reading file")
        index_yaml_data = None
      else:
        try:
          index_yaml_data = fh.read()
        finally:
          fh.close()
      requested_indexes = []
      if index_yaml_data is not None:
        index_defs = datastore_index.ParseIndexDefinitions(index_yaml_data)
        if index_defs is not None and index_defs.indexes is not None:
          requested_indexes = datastore_index.IndexDefinitionsToProtos(
              self.__app_id,
              index_defs.indexes)
          self.__cached_yaml = (index_yaml_file, index_yaml_mtime,
                               requested_indexes)
     
    existing_indexes = datastore_pb.CompositeIndices()
    app_str = api_base_pb.StringProto()
    app_str.set_value(self.__app_id)
    self._Dynamic_GetIndices(app_str, existing_indexes)

    requested = dict((x.definition().Encode(), x) for x in requested_indexes)
    existing = dict((x.definition().Encode(), x) for x in 
      existing_indexes.index_list())

    # Delete any indexes that are no longer requested.
    deleted = 0
    for key, index in existing.iteritems():
      if key not in requested:
        self._Dynamic_DeleteIndex(index, api_base_pb.VoidProto())
        deleted += 1

    # Add existing indexes in the index cache.
    for key, index in existing.iteritems():
      new_index = entity_pb.CompositeIndex()
      new_index.CopyFrom(index)
      ent_kind = new_index.definition().entity_type()
      if ent_kind in self.__index_cache:
        new_indexes = self.__index_cache[ent_kind]
        new_indexes.append(new_index)
        self.__index_cache[ent_kind] = new_indexes
      else:
        self.__index_cache[ent_kind] = [new_index]
  
    # Compared the existing indexes to the requested ones and create any
    # new indexes requested.
    created = 0
    for key, index in requested.iteritems():
      if key not in existing:
        new_index = entity_pb.CompositeIndex()
        new_index.CopyFrom(index)
        new_index.set_id(self._Dynamic_CreateIndex(new_index, 
          api_base_pb.Integer64Proto()).value())
        new_index.set_state(entity_pb.CompositeIndex.READ_WRITE)
        self._Dynamic_UpdateIndex(new_index, api_base_pb.VoidProto())
        created += 1
  
        ent_kind = new_index.definition().entity_type()
        if ent_kind in self.__index_cache:
          new_indexes = self.__index_cache[ent_kind]
     
          new_indexes.append(new_index)
          self.__index_cache[ent_kind] = new_indexes
        else:
          self.__index_cache[ent_kind] = [new_index]

    if created or deleted:
      logging.info('Created %d and deleted %d index(es); total %d',
                    created, deleted, len(requested))

def _FindIndexToUse(query, indexes):
  """ Matches the query with one of the composite indexes. 

  Args:
    query: A datastore_pb.Query.
    indexes: A list of entity_pb.CompsiteIndex.
  Returns:
    The composite index of the list for which the composite index matches 
    the query. Returns None if there is no match.
  """
  if not query.has_kind():
    return None

  index_list = __IndexListForQuery(query)
  if index_list == []:
    return None

  index_match = index_list[0]
  for index in indexes:
    if index_match.Equals(index.definition()):
      return index

  raise apiproxy_errors.ApplicationError(
    datastore_pb.Error.NEED_INDEX,
    'Query requires an index')

def __IndexListForQuery(query):
  """Get the composite index definition used by the query, if any, as a list.

  Args:
    query: the datastore_pb.Query to compute the index list for

  Returns:
    A singleton list of the composite index definition pb used by the query,
  """
  required, kind, ancestor, props = (
      datastore_index.CompositeIndexForQuery(query))
  if not required:
    return []

  index_pb = entity_pb.Index()
  index_pb.set_entity_type(kind)
  index_pb.set_ancestor(bool(ancestor))
  for name, direction in datastore_index.GetRecommendedIndexProperties(props):
    prop_pb = entity_pb.Index_Property()
    prop_pb.set_name(name)
    prop_pb.set_direction(direction)
    index_pb.property_list().append(prop_pb)
  return [index_pb]


