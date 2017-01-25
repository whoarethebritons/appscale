#!/usr/bin/env python

""" A service for handling TaskQueue request from application servers.
It uses RabbitMQ and Celery to handle tasks. """

import base64
import datetime
import hashlib
import json
import os
import socket
import sys
import time
import tq_lib

from cassandra import OperationTimedOut
from cassandra.cluster import SimpleStatement
from cassandra.policies import FallthroughRetryPolicy
from distutils.spawn import find_executable
from queue import InvalidLeaseRequest
from queue import PullQueue
from queue import PushQueue
from task import Task
from tq_config import TaskQueueConfig
from .unpackaged import APPSCALE_LIB_DIR
from .unpackaged import APPSCALE_PYTHON_APPSERVER
from .utils import logger

sys.path.append(APPSCALE_LIB_DIR)
import appscale_info
import constants
import file_io
import monit_app_configuration
import monit_interface

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_errors
from google.appengine.api import datastore_distributed
from google.appengine.api.taskqueue import taskqueue_service_pb
from google.appengine.ext import db
from google.appengine.runtime import apiproxy_errors

sys.path.append(TaskQueueConfig.CELERY_WORKER_DIR)

# A policy that does not retry statements.
NO_RETRIES = FallthroughRetryPolicy()


def create_pull_queue_tables(cluster, session):
  """ Create the required tables for pull queues.

  Args:
    cluster: A cassandra-driver cluster.
    session: A cassandra-driver session.
  """
  logger.info('Trying to create pull_queue_tasks')
  create_table = """
    CREATE TABLE IF NOT EXISTS pull_queue_tasks (
      app text,
      queue text,
      id text,
      payload text,
      enqueued timestamp,
      lease_expires timestamp,
      retry_count int,
      tag text,
      PRIMARY KEY ((app, queue, id))
    )
  """
  statement = SimpleStatement(create_table, retry_policy=NO_RETRIES)
  try:
    session.execute(statement)
  except OperationTimedOut:
    logger.warning(
      'Encountered an operation timeout while creating pull_queue_tasks. '
      'Waiting 1 minute for schema to settle.')
    time.sleep(60)
    raise

  logger.info('Trying to create pull_queue_tasks_index')
  create_index_table = """
    CREATE TABLE IF NOT EXISTS pull_queue_tasks_index (
      app text,
      queue text,
      eta timestamp,
      id text,
      tag text,
      tag_exists boolean,
      PRIMARY KEY ((app, queue, eta), id)
    )
  """
  statement = SimpleStatement(create_index_table, retry_policy=NO_RETRIES)
  try:
    session.execute(statement)
  except OperationTimedOut:
    logger.warning(
      'Encountered an operation timeout while creating pull_queue_tasks_index.'
      ' Waiting 1 minute for schema to settle.')
    time.sleep(60)
    raise

  logger.info('Trying to create pull_queue_tags index')
  create_index = """
    CREATE INDEX IF NOT EXISTS pull_queue_tags ON pull_queue_tasks_index (tag);
  """
  session.execute(create_index)

  # This additional index is needed for groupByTag=true,tag=None queries
  # because Cassandra can only do '=' queries on secondary indices.
  logger.info('Trying to create pull_queue_tag_exists index')
  create_index = """
    CREATE INDEX IF NOT EXISTS pull_queue_tag_exists
    ON pull_queue_tasks_index (tag_exists);
  """
  session.execute(create_index)

  logger.info('Trying to create pull_queue_leases')
  create_leases_table = """
    CREATE TABLE IF NOT EXISTS pull_queue_leases (
      app text,
      queue text,
      leased timestamp,
      PRIMARY KEY ((app, queue, leased))
    )
  """
  statement = SimpleStatement(create_leases_table, retry_policy=NO_RETRIES)
  try:
    session.execute(statement)
  except OperationTimedOut:
    logger.warning(
      'Encountered an operation timeout while creating pull_queue_leases. '
      'Waiting 1 minute for schema to settle.')
    time.sleep(60)
    raise


class TaskName(db.Model):
  """ A datastore model for tracking task names in order to prevent
  tasks with the same name from being enqueued repeatedly.
  
  Attributes:
    timestamp: The time the task was enqueued.
  """
  STORED_KIND_NAME = "__task_name__"
  timestamp = db.DateTimeProperty(auto_now_add=True)
  queue = db.StringProperty(required=True)
  state = db.StringProperty(required=True)
  endtime = db.DateTimeProperty()
  app_id = db.StringProperty(required=True)

  @classmethod
  def kind(cls):
    """ Kind name override. """
    return cls.STORED_KIND_NAME

def setup_env():
  """ Sets required environment variables for GAE datastore library. """
  os.environ['AUTH_DOMAIN'] = "appscale.com"
  os.environ['USER_EMAIL'] = ""
  os.environ['USER_NICKNAME'] = ""
  os.environ['APPLICATION_ID'] = ""

class DistributedTaskQueue():
  """ AppScale taskqueue layer for the TaskQueue API. """

  # Required start worker name tags.
  SETUP_WORKERS_TAGS = ['app_id']

  # Required stop worker name tags.
  STOP_WORKERS_TAGS = ['app_id']

  # The location of where celery logs go.
  LOG_DIR = "/var/log/appscale/celery_workers/"

  # The hard time limit of a running task in seconds, extra
  # time over the soft limit allows it to catch up to interrupts.
  HARD_TIME_LIMIT = 610

  # The soft time limit of a running task.
  TASK_SOFT_TIME_LIMIT = 600

  # The location where celery tasks place their PID file. Prevents
  # the same worker from being started if it is already running.
  PID_FILE_LOC = "/etc/appscale/"

  # A port number given to the monitoring service, but not actually used.
  CELERY_PORT = 9999

  # The longest a task is allowed to run in days.
  DEFAULT_EXPIRATION = 30

  # The default maximum number to retry a task, where 0 or None is unlimited.
  DEFAULT_MAX_RETRIES = 0

  # The default amount of min/max time we wait before retrying a task
  # in seconds.
  DEFAULT_MIN_BACKOFF = 1
  DEFAULT_MAX_BACKOFF = 3600.0

  # Default number of times we double the backoff value.
  DEFAULT_MAX_DOUBLINGS = 1000

  # Kind used for storing task names.
  TASK_NAME_KIND = "__task_name__"

  # A dict that tells celery to run tasks even though we are running as root.
  CELERY_ENV_VARS = {"C_FORCE_ROOT" : True}

  # The max memory allocated to celery worker pools in MB.
  CELERY_MAX_MEMORY = 1000

  def __init__(self, db_access):
    """ DistributedTaskQueue Constructor.

    Args:
      db_access: A DatastoreProxy object.
    """
    file_io.mkdir(self.LOG_DIR)
    file_io.mkdir(TaskQueueConfig.CELERY_WORKER_DIR)
    file_io.mkdir(TaskQueueConfig.CELERY_CONFIG_DIR)

    setup_env()
  
    # Cache all queue information in memory.
    self.__queue_info_cache = {}

    master_db_ip = appscale_info.get_db_master_ip()
    connection_str = master_db_ip + ":" + str(constants.DB_SERVER_PORT)
    ds_distrib = datastore_distributed.DatastoreDistributed(
      constants.DASHBOARD_APP_ID, connection_str, require_indexes=False)
    apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', ds_distrib)
    os.environ['APPLICATION_ID'] = constants.DASHBOARD_APP_ID

    self.db_access = db_access

    # Flag to see if code needs to be reloaded.
    self.__force_reload = False

  def get_queue(self, app, queue):
    """ Fetches a Queue object.

    Args:
      app: A string containing the application ID.
      queue: A string specifying the name of the queue.
    Returns:
      A Queue object or None.
    """
    cache = self.__queue_info_cache
    if app in cache and queue in cache[app]:
      return cache[app][queue]

    config = TaskQueueConfig(app, self.db_access)
    self.__queue_info_cache[app] = config.queues
    if queue in config.queues:
      return config.queues[queue]
    else:
      return None

  def __parse_json_and_validate_tags(self, json_request, tags):
    """ Parses JSON and validates that it contains the proper tags.

    Args: 
      json_request: A JSON string.
      tags: The tags to validate if they are in the JSON string.
    Returns:
      A dictionary dumped from the JSON string.
    """
    try:
      json_response = json.loads(json_request)
    except ValueError:
      json_response = {"error": True, 
                       "reason": "Badly formed JSON"}
      return json_response

    for tag in tags:
      if tag  not in json_response:
        json_response = {'error': True, 
                         'reason': 'Missing ' + tag + ' tag'}
        break
    return json_response

  def stop_worker(self, json_request):
    """ Stops the monit watch for queues of an application on the current node.
   
    Args:
      json_request: A JSON string with the queue name for which we're
        stopping its queues.
    Returns:
      A JSON string with the result.
    """
    request = self.__parse_json_and_validate_tags(
      json_request, self.STOP_WORKERS_TAGS)
    logger.info("Stopping worker: {0}".format(request))
    if 'error' in request:
      return json.dumps(request)

    app_id = request['app_id']
    watch = "celery-" + str(app_id)
    try:
      if monit_interface.stop(watch):
        stop_command = self.get_worker_stop_command(app_id)
        os.system(stop_command) 
        TaskQueueConfig.remove_config_files(app_id)
        result = {'error': False}
      else:
        result = {'error': True, 'reason': "Unable to stop watch %s" % watch}
    except OSError, os_error:
      result = {'error': True, 'reason' : str(os_error)}

    return json.dumps(result)
    
  def get_worker_stop_command(self, app_id):
    """ Returns the command to run to stop celery workers for a given
    application.
  
    Args:
      app_id: The application ID.
    Returns:
      A string which, if run, will kill celery workers for a given
      application ID.
    """
    stop_command = "/usr/bin/python2 {0}/scripts/stop_service.py worker {1}" \
      .format(constants.APPSCALE_HOME, app_id)
    return stop_command

  def reload_worker(self, json_request):
    """ Reloads taskqueue workers as needed. A worker can be started on both
    a master and slave node.
 
    Args:
      json_request: A JSON string with the application ID.
    Returns:
      A JSON string with the error status and error reason.
    """
    request = self.__parse_json_and_validate_tags(json_request,  
                                         self.SETUP_WORKERS_TAGS)
    logger.info("Reload worker request: {0}".format(request))
    if 'error' in request:
      return json.dumps(request)

    app_id = self.__cleanse(request['app_id'])

    cached_queues = {}
    if app_id in self.__queue_info_cache:
      cached_queues = self.__queue_info_cache[app_id]

    try:
      new_queues = TaskQueueConfig(app_id, self.db_access).queues
    except (ValueError, NameError) as config_error:
      return json.dumps({'error': True, 'reason': config_error.message})
    except Exception as config_error:
      logger.exception('Unknown exception')
      return json.dumps({'error': True, 'reason': config_error.message})

    reload_workers = False

    # Stop workers for push queues that no longer exist.
    for name, queue in cached_queues.iteritems():
      if not isinstance(queue, PushQueue):
        continue

      if name not in new_queues:
        logger.info('Deleting queue for {}: {}'.format(app_id, name))
        reload_workers = True

    # Create any new push queues and update ones that have changed.
    for name, queue in new_queues.iteritems():
      if not isinstance(queue, PushQueue):
        continue

      if name not in cached_queues:
        logger.info('Creating queue for {}: {}'.format(app_id, name))
        reload_workers = True
        continue

      if queue != cached_queues[name]:
        logger.info('Reloading queue for {}: {}'.format(app_id, name))
        logger.debug('Old: {}\nNew: {}'.format(cached_queues[name], queue))
        reload_workers = True

    if reload_workers:
      self.stop_worker(json_request)
      self.start_worker(json_request)
      self.__force_reload = True
    else:
      logger.info('Not reloading queues')

    self.__queue_info_cache[app_id] = new_queues

    json_response = {'error': False}
    return json.dumps(json_response)

  def start_worker(self, json_request):
    """ Starts taskqueue workers if they are not already running. A worker
    can be started on both a master and slave node.
 
    Args:
      json_request: A JSON string with the application id.
    Returns:
      A JSON string with the error status and error reason.
    """
    request = self.__parse_json_and_validate_tags(json_request,  
                                         self.SETUP_WORKERS_TAGS)
    logger.info("Start worker request: {0}".format(request))
    if 'error' in request:
      return json.dumps(request)

    app_id = self.__cleanse(request['app_id'])

    # Load the queue info.
    try:
      config = TaskQueueConfig(app_id, self.db_access)
      self.__queue_info_cache[app_id] = config.queues
      config.create_celery_file()
      config.create_celery_worker_scripts()
    except (ValueError, NameError) as config_error:
      return json.dumps({'error': True, 'reason': config_error.message})
    except Exception as config_error:
      logger.exception('Unknown exception')
      return json.dumps({'error': True, 'reason': config_error.message})
   
    log_file = self.LOG_DIR + app_id + ".log"
    celery_bin = find_executable('celery')
    command = [celery_bin,
               "worker",
               "--app=" + \
                    TaskQueueConfig.get_celery_worker_module_name(app_id),
               "--hostname=%h." + app_id,
               "--workdir=" + TaskQueueConfig.CELERY_WORKER_DIR,
               "--logfile=" + log_file,
               "--time-limit=" + str(self.HARD_TIME_LIMIT),
               "--autoscale={max},{min}".format(
                 max=TaskQueueConfig.MAX_CELERY_CONCURRENCY,
                 min=TaskQueueConfig.MIN_CELERY_CONCURRENCY),
               "--soft-time-limit=" + str(self.TASK_SOFT_TIME_LIMIT),
               "--pidfile=" + self.PID_FILE_LOC + 'celery___' + \
                             app_id + ".pid",
               "--statedb=" + TaskQueueConfig.CELERY_STATE_DIR + 'worker___' + \
                             app_id + ".db",
               "--autoreload -Ofair"]
    start_command = str(' '.join(command))
    stop_command = self.get_worker_stop_command(app_id)
    watch = "celery-" + str(app_id)
    monit_app_configuration.create_config_file(watch,
                                               start_command, 
                                               stop_command, 
                                               [self.CELERY_PORT],
                                               max_memory=self.CELERY_MAX_MEMORY,
                                               env_vars=self.CELERY_ENV_VARS)
    if monit_interface.start(watch):
      json_response = {'error': False}
    else:
      json_response = {'error': True, 
                       'reason': "Start of monit watch for %s failed" % watch}
    return json.dumps(json_response)

  def fetch_queue_stats(self, app_id, http_data):
    """ Gets statistics about tasks in queues.

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    epoch = datetime.datetime.utcfromtimestamp(0)
    request = taskqueue_service_pb.TaskQueueFetchQueueStatsRequest(http_data)
    response = taskqueue_service_pb.TaskQueueFetchQueueStatsResponse()

    for queue_name in request.queue_name_list():
      queue = self.get_queue(app_id, queue_name)
      stats_response = response.add_queuestats()

      if isinstance(queue, PullQueue):
        num_tasks = queue.total_tasks()
        oldest_eta = queue.oldest_eta()
      else:
        num_tasks = TaskName.all().\
          filter("state =", tq_lib.TASK_STATES.QUEUED).\
          filter("queue =", queue_name).\
          filter("app_id =", app_id).count()

        # This is not supported for push queues yet.
        oldest_eta = None

      # -1 is used to indicate an absence of a value.
      oldest_eta_usec = (int((oldest_eta - epoch).total_seconds() * 1000000)
                         if oldest_eta else -1)

      stats_response.set_num_tasks(num_tasks)
      stats_response.set_oldest_eta_usec(oldest_eta_usec)

    return response.Encode(), 0, ""

  def purge_queue(self, app_id, http_data):
    """ 

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    request = taskqueue_service_pb.TaskQueuePurgeQueueRequest(http_data)
    response = taskqueue_service_pb.TaskQueuePurgeQueueResponse()

    queue = self.get_queue(app_id, request.queue_name())
    queue.purge()
    return (response.Encode(), 0, "")

  def delete(self, app_id, http_data):
    """ Delete a task.

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    request = taskqueue_service_pb.TaskQueueDeleteRequest(http_data)
    response = taskqueue_service_pb.TaskQueueDeleteResponse()

    queue = self.get_queue(app_id, request.queue_name())
    for task_name in request.task_name_list():
      queue.delete_task(Task({'id': task_name}))
      response.add_result(taskqueue_service_pb.TaskQueueServiceError.OK)

    return response.Encode(), 0, ""

  def query_and_own_tasks(self, app_id, http_data):
    """ Lease pull queue tasks.

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    request = taskqueue_service_pb.TaskQueueQueryAndOwnTasksRequest(http_data)
    response = taskqueue_service_pb.TaskQueueQueryAndOwnTasksResponse()

    queue = self.get_queue(app_id, request.queue_name())
    tag = None
    if request.has_tag():
      tag = request.tag()

    tasks = queue.lease_tasks(request.max_tasks(), request.lease_seconds(),
                              group_by_tag=request.group_by_tag(), tag=tag)
    for task in tasks:
      task_pb = response.add_task()
      task_pb.MergeFrom(task.encode_lease_pb())

    return response.Encode(), 0, ""

  def add(self, app_id, http_data):
    """ Adds a single task to the task queue.

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    # Just call bulk add with one task.
    request = taskqueue_service_pb.TaskQueueAddRequest(http_data)
    request.set_app_id(app_id)
    response = taskqueue_service_pb.TaskQueueAddResponse()
    bulk_request = taskqueue_service_pb.TaskQueueBulkAddRequest()
    bulk_response = taskqueue_service_pb.TaskQueueBulkAddResponse()
    bulk_request.add_add_request().CopyFrom(request)

    self.__bulk_add(bulk_request, bulk_response) 

    if bulk_response.taskresult_size() == 1:
      result = bulk_response.taskresult(0).result()
    else:
      err_code = taskqueue_service_pb.TaskQueueServiceError.INTERNAL_ERROR 
      return (response.Encode(), err_code, 
              "Task did not receive a task response.")

    if result != taskqueue_service_pb.TaskQueueServiceError.OK:
      return (response.Encode(), result, "Task did not get an OK status.")
    elif bulk_response.taskresult(0).has_chosen_task_name():
      response.set_chosen_task_name(
             bulk_response.taskresult(0).chosen_task_name())

    return (response.Encode(), 0, "")

  def bulk_add(self, app_id, http_data):
    """ Adds multiple tasks to the task queue.

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    request = taskqueue_service_pb.TaskQueueBulkAddRequest(http_data)
    response = taskqueue_service_pb.TaskQueueBulkAddResponse()
    self.__bulk_add(request, response)
    return (response.Encode(), 0, "")

  def __bulk_add(self, request, response):
    """ Function for bulk adding tasks.
   
    Args:
      request: taskqueue_service_pb.TaskQueueBulkAddRequest.
      response: taskqueue_service_pb.TaskQueueBulkAddResponse.
    Raises:
      apiproxy_error.ApplicationError.
    """
    if request.add_request_size() == 0:
      return
   
    now = datetime.datetime.utcfromtimestamp(time.time())

    # Assign names if needed and validate tasks.
    error_found = False
    for add_request in request.add_request_list():
      task_result = response.add_taskresult()

      if (add_request.has_mode() and
          add_request.mode() == taskqueue_service_pb.TaskQueueMode.PULL):
        queue = self.get_queue(add_request.app_id(), add_request.queue_name())
        if not isinstance(queue, PullQueue):
          task_result.set_result(
            taskqueue_service_pb.TaskQueueServiceError.INVALID_QUEUE_MODE)
          error_found = True

        encoded_payload = base64.urlsafe_b64encode(add_request.body())
        task_info = {'payloadBase64': encoded_payload,
                     'leaseTimestamp': add_request.eta_usec()}
        if add_request.has_task_name():
          task_info['id'] = add_request.task_name()
        if add_request.has_tag():
          task_info['tag'] = add_request.tag()

        new_task = Task(task_info)
        queue.add_task(new_task)
        task_result.set_result(taskqueue_service_pb.TaskQueueServiceError.OK)
        task_result.set_chosen_task_name(new_task.id)
        continue

      result = tq_lib.verify_task_queue_add_request(add_request.app_id(),
                                                    add_request, now)
      # Tasks go from SKIPPED to OK once they're run. If there are
      # any failures from other tasks then we pass this request 
      # back as skipped.
      if result == taskqueue_service_pb.TaskQueueServiceError.SKIPPED:
        task_name = None       
        if add_request.has_task_name():
          task_name = add_request.task_name()
           
        namespaced_name = tq_lib.choose_task_name(add_request.app_id(),
                                                  add_request.queue_name(),
                                                  user_chosen=task_name)
        add_request.set_task_name(namespaced_name)
        task_result.set_chosen_task_name(namespaced_name)
      else:
        error_found = True
        task_result.set_result(result)
    if error_found:
      return

    for add_request, task_result in zip(request.add_request_list(),
                                        response.taskresult_list()):
      if (add_request.has_mode() and
          add_request.mode() == taskqueue_service_pb.TaskQueueMode.PULL):
        continue

      try:
        self.__enqueue_push_task(add_request)
      except apiproxy_errors.ApplicationError as error:
        task_result.set_result(error.application_error)
      else:
        task_result.set_result(taskqueue_service_pb.TaskQueueServiceError.OK)

  def __method_mapping(self, method):
    """ Maps an int index to a string. 
   
    Args:
      method: int representing a http method.
    Returns:
      A string version of the method.
   """
    if method == taskqueue_service_pb.TaskQueueQueryTasksResponse_Task.GET:
      return 'GET'
    elif method == taskqueue_service_pb.TaskQueueQueryTasksResponse_Task.POST:
      return  'POST'
    elif method == taskqueue_service_pb.TaskQueueQueryTasksResponse_Task.HEAD:
      return  'HEAD'
    elif method == taskqueue_service_pb.TaskQueueQueryTasksResponse_Task.PUT:
      return 'PUT'
    elif method == taskqueue_service_pb.TaskQueueQueryTasksResponse_Task.DELETE:
      return 'DELETE'

  def __check_and_store_task_names(self, request):
    """ Tries to fetch the taskqueue name, if it exists it will raise an 
    exception. 

    We store a receipt of each enqueued task in the datastore. If we find that
    task in the datastore, we will raise an exception. If the task is not
    in the datastore, then it is assumed this is the first time seeing the
    tasks and we create a receipt of the task in the datastore to prevent
    a duplicate task from being enqueued.
    
    Args:
      request: A taskqueue_service_pb.TaskQueueAddRequest.
    Raises:
      A apiproxy_errors.ApplicationError of TASK_ALREADY_EXISTS.
    """
    task_name = request.task_name()
    item = TaskName.get_by_key_name(task_name)
    logger.debug("Task name {0}".format(task_name))
    if item:
      logger.warning("Task already exists")
      raise apiproxy_errors.ApplicationError(
        taskqueue_service_pb.TaskQueueServiceError.TASK_ALREADY_EXISTS)
    else:
      new_name = TaskName(key_name=task_name, state=tq_lib.TASK_STATES.QUEUED,
        queue=request.queue_name(), app_id=request.app_id())
      logger.debug("Creating entity {0}".format(str(new_name)))
      try:
        db.put(new_name)
      except datastore_errors.InternalError, internal_error:
        logger.error(str(internal_error))
        raise apiproxy_errors.ApplicationError(
          taskqueue_service_pb.TaskQueueServiceError.DATASTORE_ERROR)

  def __enqueue_push_task(self, request):
    """ Enqueues a batch of push tasks.
  
    Args:
      request: A taskqueue_service_pb.TaskQueueAddRequest.
    """
    self.__validate_push_task(request)
    self.__check_and_store_task_names(request)
    args = self.get_task_args(request)
    headers = self.get_task_headers(request)
    countdown = int(headers['X-AppEngine-TaskETA']) - \
                int(datetime.datetime.now().strftime("%s"))
    task_func = self.__get_task_function(request)
    task_func.apply_async(
      kwargs={'headers':headers, 'args':args},
      expires=args['expires'],
      acks_late=True,
      countdown=countdown,
      queue=TaskQueueConfig.get_celery_queue_name(request.app_id(),
                                                  request.queue_name()),
      routing_key=TaskQueueConfig.get_celery_queue_name(request.app_id(),
                                                        request.queue_name()))

  def __get_task_function(self, request):
    """ Returns a function pointer to a celery task. Loads the module for the
    app/queue.

    Args:
      request: A taskqueue_service_pb.TaskQueueAddRequest.
    Returns:
      A function pointer to a celery task.
    Raises:
      taskqueue_service_pb.TaskQueueServiceError.
    """
    try:
      task_module = __import__(TaskQueueConfig.\
                  get_celery_worker_module_name(request.app_id()))

      # If a new queue was added we need to relaod the python code.
      if self.__force_reload:
        start = time.time()
        reload(task_module)
        time_taken = time.time() - start
        self.__force_reload = False
        logger.info("Reloading module for {0} took {1} seconds.".\
          format(request.app_id(), time_taken))

      task_func = getattr(task_module, 
        TaskQueueConfig.get_queue_function_name(request.queue_name()))
      return task_func
    except AttributeError, attribute_error:
      logger.exception(attribute_error)
      raise apiproxy_errors.ApplicationError(
              taskqueue_service_pb.TaskQueueServiceError.UNKNOWN_QUEUE)
    except ImportError, import_error:
      logger.exception(import_error)
      raise apiproxy_errors.ApplicationError(
              taskqueue_service_pb.TaskQueueServiceError.UNKNOWN_QUEUE)

  def get_task_args(self, request):
    """ Gets the task args used when making a task web request.
  
    Args:
      request: A taskqueue_service_pb.TaskQueueAddRequest.
    Returns:
      A dictionary used by a task worker.
    """
    args = {}
    args['task_name'] = request.task_name()
    args['url'] = request.url()
    args['app_id'] = request.app_id()
    args['queue_name'] = request.queue_name()
    args['method'] = self.__method_mapping(request.method())
    args['body'] = request.body()
    args['payload'] = request.payload()
    args['description'] = request.description()

    # Set defaults.
    args['max_retries'] = self.DEFAULT_MAX_RETRIES
    args['expires'] = self.__when_to_expire(request)
    args['max_retries'] = self.DEFAULT_MAX_RETRIES
    args['max_backoff_sec'] = self.DEFAULT_MAX_BACKOFF 
    args['min_backoff_sec'] = self.DEFAULT_MIN_BACKOFF 
    args['max_doublings'] = self.DEFAULT_MAX_DOUBLINGS

    # Load queue info into cache.
    app_id = self.__cleanse(request.app_id())
    queue_name = request.queue_name()
    if app_id not in self.__queue_info_cache:
      try:
        self.__queue_info_cache[app_id] = TaskQueueConfig(
          app_id, self.db_access).queues
      except (ValueError, NameError):
        logger.exception('Unable to load queues for {}. Using defaults.'\
          .format(app_id))
      except Exception:
        logger.exception('Unknown exception')
  
    # Use queue defaults.
    if (app_id in self.__queue_info_cache and
        queue_name in self.__queue_info_cache[app_id]):
      queue = self.__queue_info_cache[app_id][queue_name]
      if not isinstance(queue, PushQueue):
        raise Exception('Only push queues are implemented')

      args['max_retries'] = queue.task_retry_limit
      args['min_backoff_sec'] = queue.min_backoff_seconds
      args['max_backoff_sec'] = queue.max_backoff_seconds
      args['max_doublings'] = queue.max_doublings

    # Override defaults.
    if request.has_retry_parameters():
      retry_params = request.retry_parameters()
      if retry_params.has_retry_limit():
        args['max_retries'] = retry_params.retry_limit()
      if retry_params.has_min_backoff_sec():
        args['min_backoff_sec'] = request.\
                                  retry_parameters().min_backoff_sec()
      if retry_params.has_max_backoff_sec():
        args['max_backoff_sec'] = request.\
                                  retry_parameters().max_backoff_sec()
      if retry_params.has_max_doublings():
        args['max_doublings'] = request.\
                                  retry_parameters().max_doublings()
    return args

  def get_task_headers(self, request):
    """ Gets the task headers used for a task web request. 

    Args:
      request: A taskqueue_service_pb.TaskQueueAddRequest
    Returns:
      A dictionary of key/values for a web request.
    """  
    headers = {}
    for header in request.header_list():
      headers[header.key()] = header.value()

    eta = self.__when_to_run(request)
    
    # This header is how we authenticate that it's an internal request
    secret = appscale_info.get_secret() 
    secret_hash = hashlib.sha1(request.app_id() + '/' + \
                      secret).hexdigest()
    headers['X-AppEngine-Fake-Is-Admin'] = secret_hash
    headers['X-AppEngine-QueueName'] = request.queue_name()
    headers['X-AppEngine-TaskName'] = request.task_name()
    headers['X-AppEngine-TaskRetryCount'] = '0'
    headers['X-AppEngine-TaskExecutionCount'] = '0'
    headers['X-AppEngine-TaskETA'] = str(int(eta.strftime("%s")))
    return headers

  def __when_to_run(self, request):
    """ Returns a datetime object of when a task should execute.
    
    Args:
      request: A taskqueue_service_pb.TaskQueueAddRequest.
    Returns:
      A datetime object for when the nearest time to run the 
     task is.
    """
    if request.has_eta_usec():
      eta = request.eta_usec()
      return datetime.datetime.fromtimestamp(eta/1000000)
    else:
      return datetime.datetime.now() 

  def __when_to_expire(self, request):
    """ Returns a datetime object of when a task should expire.
    
    Args:
      request: A taskqueue_service_pb.TaskQueueAddRequest.
    Returns:
      A datetime object of when the task should expire. 
    """
    if request.has_retry_parameters() and \
           request.retry_parameters().has_age_limit_sec():
      limit = request.retry_parameters().age_limit_sec()
      return datetime.datetime.now() + datetime.timedelta(seconds=limit)
    else:
      return datetime.datetime.now() + \
                   datetime.timedelta(days=self.DEFAULT_EXPIRATION)

  def __validate_push_task(self, request):
    """ Checks to make sure the task request is valid.
    
    Args:
      request: A taskqueue_service_pb.TaskQueueAddRequest. 
    Raises:
      apiproxy_errors.ApplicationError upon invalid tasks.
    """ 
    if not request.has_queue_name():
      raise apiproxy_errors.ApplicationError(
              taskqueue_service_pb.TaskQueueServiceError.INVALID_QUEUE_NAME)
    if not request.has_task_name():
      raise apiproxy_errors.ApplicationError(
              taskqueue_service_pb.TaskQueueServiceError.INVALID_TASK_NAME)
    if not request.has_app_id():
      raise apiproxy_errors.ApplicationError(
              taskqueue_service_pb.TaskQueueServiceError.UNKNOWN_QUEUE)
    if not request.has_url():
      raise apiproxy_errors.ApplicationError(
              taskqueue_service_pb.TaskQueueServiceError.INVALID_URL)
    if request.has_mode() and request.mode() == \
              taskqueue_service_pb.TaskQueueMode.PULL:
      raise apiproxy_errors.ApplicationError(
              taskqueue_service_pb.TaskQueueServiceError.INVALID_QUEUE_MODE)
     
  def modify_task_lease(self, app_id, http_data):
    """ 

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    request = taskqueue_service_pb.TaskQueueModifyTaskLeaseRequest(http_data)
    response = taskqueue_service_pb.TaskQueueModifyTaskLeaseResponse()

    queue = self.get_queue(app_id, request.queue_name())
    task_info = {'id': request.task_name()}
    try:
      # The Python AppServer sets eta_usec with a resolution of 1 second,
      # so update_lease can't be used. It checks with millisecond precision.
      task = queue.update_task(Task(task_info), request.lease_seconds())
    except InvalidLeaseRequest as lease_error:
      error = taskqueue_service_pb.TaskQueueServiceError.TASK_LEASE_EXPIRED
      # The response requires ETA to be set before encoding.
      response.set_updated_eta_usec(0)
      return response.Encode(), error, str(lease_error)

    epoch = datetime.datetime.utcfromtimestamp(0)
    updated_usec = int((task.leaseTimestamp - epoch).total_seconds() * 1000000)
    response.set_updated_eta_usec(updated_usec)
    return response.Encode(), 0, ""

  def fetch_queue(self, app_id, http_data):
    """ 

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    # TODO implement.
    request = taskqueue_service_pb.TaskQueueFetchQueuesRequest(http_data)
    response = taskqueue_service_pb.TaskQueueFetchQueuesResponse()
    return (response.Encode(), 0, "")

  def query_tasks(self, app_id, http_data):
    """ 

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    # TODO implement.
    request = taskqueue_service_pb.TaskQueueQueryTasksRequest(http_data)
    response = taskqueue_service_pb.TaskQueueQueryTasksResponse()
    return (response.Encode(), 0, "")

  def fetch_task(self, app_id, http_data):
    """ 

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    # TODO implement.
    request = taskqueue_service_pb.TaskQueueFetchTaskRequest(http_data)
    response = taskqueue_service_pb.TaskQueueFetchTaskResponse()
    return (response.Encode(), 0, "")

  def force_run(self, app_id, http_data):
    """ 

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    # TODO implement.
    request = taskqueue_service_pb.TaskQueueForceRunRequest(http_data)
    response = taskqueue_service_pb.TaskQueueForceRunResponse()
    return (response.Encode(), 0, "")

  def pause_queue(self, app_id, http_data):
    """ 

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    # TODO implement.
    request = taskqueue_service_pb.TaskQueuePauseQueueRequest(http_data)
    response = taskqueue_service_pb.TaskQueuePauseQueueResponse()
    return (response.Encode(), 0, "")

  def delete_group(self, app_id, http_data):
    """ 

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    # TODO implement.
    request = taskqueue_service_pb.TaskQueueDeleteGroupRequest(http_data)
    response = taskqueue_service_pb.TaskQueueDeleteGroupResponse()
    return (response.Encode(), 0, "")

  def update_storage_limit(self, app_id, http_data):
    """ 

    Args:
      app_id: The application ID.
      http_data: The payload containing the protocol buffer request.
    Returns:
      A tuple of a encoded response, error code, and error detail.
    """
    # TODO implement.
    request = taskqueue_service_pb.TaskQueueUpdateStorageLimitRequest(http_data)
    response = taskqueue_service_pb.TaskQueueUpdateStorageLimitResponse()
    return (response.Encode(), 0, "")

  def __cleanse(self, str_input):
    """ Removes any questionable characters which might be apart of a remote
    attack.
   
    Args:
      str_input: The string to cleanse.
    Returns: 
      A string which has questionable characters replaced.
    """ 
    for char in "~./\\!@#$%&*()]\+=|":
      str_input = str_input.replace(char, "_")
    return str_input

  def __is_localhost(self, hostname):
    """ Determines if the hostname is that of the current host.
 
    Args:
      hostname: A string representing the hostname.
    Returns:
      True if its the localhost, false otherwise.
    """
    if socket.gethostname() == hostname:
      return True
    elif socket.gethostbyname(socket.gethostname()) == hostname:
      return True
    else:
      return False
