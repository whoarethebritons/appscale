""" A Celery worker script that executes push tasks with HTTP requests. """
import datetime
import eventlet
import json
import logging
import os
import random
import sys

from appscale.common import appscale_info
from appscale.common import constants
from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from celery.utils.log import get_task_logger
from eventlet.green import httplib
from eventlet.green.httplib import BadStatusLine
from eventlet.timeout import Timeout as EventletTimeout
from socket import error as SocketError
from urlparse import urlparse
from .distributed_tq import TaskName
from .utils import (
  create_celery_for_app,
  get_celery_configuration_path,
  get_queue_function_name
)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_distributed
from google.appengine.ext import db


# The maximum number of seconds a task is permitted to take.
MAX_TASK_DURATION = 13 * 60

app_id = os.environ['APP_ID']
remote_host = os.environ['HOST']

with open(get_celery_configuration_path(app_id)) as config_file:
  rates = json.load(config_file)

celery = create_celery_for_app(app_id, rates)

logger = get_task_logger(__name__)
logger.setLevel(logging.INFO)

db_proxy = appscale_info.get_db_proxy()
connection_str = '{}:{}'.format(db_proxy, str(constants.DB_SERVER_PORT))
ds_distrib = datastore_distributed.DatastoreDistributed(
  'appscaledashboard', connection_str, require_indexes=False)
apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', ds_distrib)
os.environ['APPLICATION_ID'] = 'appscaledashboard'


# The maximum amount of time to wait for instances to become available.
AVAILABILITY_WAIT = datetime.timedelta(minutes=10)


def get_wait_time(retries, args):
  """ Calculates how long we should wait to execute a failed task, based on
  how many times it's failed in the past.

  Args:
    retries: An int that indicates how many times this task has failed.
    args: A dict that contains information about when the user wants to retry
      the failed task.
  Returns:
    The amount of time, in seconds, that we should wait before executing this
    task again.
  """
  min_backoff_seconds = float(args['min_backoff_sec'])
  max_doublings = int(args['max_doublings'])
  max_backoff_seconds = float(args['max_backoff_sec'])
  max_doublings = min(max_doublings, retries)
  wait_time = 2 ** (max_doublings - 1) * min_backoff_seconds
  wait_time = min(wait_time, max_backoff_seconds)
  return wait_time


def clear_task_name(task_name, retries=3):
  """ Removes the TaskName entity for a given task.

  Args:
    task_name: A string specifying the TaskName key.
    retries: An integer specifying how many times to retry the delete.
  """
  task_name_entity = TaskName.get_by_key_name(task_name)

  try:
    db.delete(task_name_entity)
  except Exception as error:
    retries -= 1
    if retries >= 0:
      logger.warning('Error deleting task name: {}. Retrying'.format(error))
      return clear_task_name(task_name, retries)

    raise


def execute_task(task, headers, args):
  """ Executes a task to a url with the given args.

  Args:
    task: A celery Task instance.
    headers: A dictionary of headers for the task.
    args: A dictionary of arguments for the request.
          Contains the task body.
  Returns:
    The status code of the task fetch upon success.
  Raises:
    The current function to retry.
  """
  start_time = datetime.datetime.utcnow()

  content_length = len(args['body'])

  loggable_args = {key: args[key] for key in args
                   if key not in ['task_name', 'body', 'payload']}
  loggable_args['body_length'] = content_length
  logger.info('Running {}\n'
              'Headers: {}\n'
              'Args: {}'.format(args['task_name'], headers, loggable_args))
  url = urlparse(args['url'])

  timeout = EventletTimeout(MAX_TASK_DURATION)
  try:
    redirects_left = 1
    while True:
      urlpath = url.path
      if url.query:
        urlpath += "?" + url.query

      method = args['method']
      if args['expires'] <= datetime.datetime.now():
        # We do this check because the expires attribute in
        # celery is not passed to retried tasks. This is a
        # documented bug in celery.
        logger.error(
          "Task %s with id %s has expired with expiration date %s" % (
           args['task_name'], task.request.id, args['expires']))
        celery.control.revoke(task.request.id)
        clear_task_name(args['task_name'])
        return

      if (args['max_retries'] != 0 and
          task.request.retries >= args['max_retries']):
        logger.error("Task %s with id %s has exceeded retries: %s" % (
          args['task_name'], task.request.id,
          args['max_retries']))
        celery.control.revoke(task.request.id)
        clear_task_name(args['task_name'])
        return
      # Targets do not get X-Forwarded-Proto from nginx, they use haproxy port.
      headers['X-Forwarded-Proto'] = url.scheme
      if url.scheme == 'http':
        connection = httplib.HTTPConnection(remote_host, url.port)
      elif url.scheme == 'https':
        connection = httplib.HTTPSConnection(remote_host, url.port)
      else:
        logger.error("Task %s tried to use url scheme %s, "
                     "which is not supported." % (
                     args['task_name'], url.scheme))

      skip_host = False
      if 'host' in headers or 'Host' in headers:
        skip_host = True

      skip_accept_encoding = False
      if 'accept-encoding' in headers or 'Accept-Encoding' in headers:
        skip_accept_encoding = True

      connection.putrequest(method,
                            urlpath,
                            skip_host=skip_host,
                            skip_accept_encoding=skip_accept_encoding)

      # Update the task headers
      headers['X-AppEngine-TaskRetryCount'] = str(task.request.retries)
      headers['X-AppEngine-TaskExecutionCount'] = str(task.request.retries)

      for header in headers:
        connection.putheader(header, headers[header])

      if 'content-type' not in headers or 'Content-Type' not in headers:
        if url.query:
          connection.putheader('content-type', 'application/octet-stream')
        else:
          connection.putheader('content-type',
                               'application/x-www-form-urlencoded')

      connection.putheader("Content-Length", str(content_length))

      retries = int(task.request.retries) + 1
      wait_time = get_wait_time(retries, args)

      try:
        connection.endheaders()
        if args["body"]:
          connection.send(args['body'])

        response = connection.getresponse()
        response.read()
        response.close()
      except (BadStatusLine, SocketError):
        logger.warning(
          '{task} failed before receiving response. It will retry in {wait} '
          'seconds.'.format(task=args['task_name'], wait=wait_time))
        raise task.retry(countdown=wait_time)

      if 200 <= response.status < 300:
        # Task successful.
        clear_task_name(args['task_name'])
        time_elapsed = datetime.datetime.utcnow() - start_time
        logger.info(
          '{task} received status {status} from {url} [time elapsed: {te}]'. \
          format(task=args['task_name'], status=response.status,
                 url=url, te=str(time_elapsed)))
        return response.status
      elif response.status == 302:
        redirect_url = response.getheader('Location')
        logger.info(
          "Task %s asked us to redirect to %s, so retrying there." % (
            args['task_name'], redirect_url))
        url = urlparse(redirect_url)
        if redirects_left == 0:
          raise task.retry(countdown=wait_time)
        redirects_left -= 1
      elif response.status == 503:
        if datetime.datetime.utcnow() - start_time > AVAILABILITY_WAIT:
          message = ('{task} waited too long for available instances. '
                     'Retrying in {wait} seconds'.format(task=task['task_name'],
                                                         wait=wait_time))
          logger.warning(message)
          raise task.retry(countdown=wait_time)

        service_backoff_time = random.randint(1, 5)
        logger.warning('No available instances. '
                       'Waiting {} seconds'.format(service_backoff_time))
        eventlet.sleep(seconds=service_backoff_time)
        continue
      else:
        message = ('Received a {status} for {task}. '
                   'Retrying in {wait} secs.'.format(status=response.status,
                                                     task=args['task_name'],
                                                     wait=wait_time))
        logger.warning(message)
        raise task.retry(countdown=wait_time)
  except EventletTimeout as thrown_timeout:
    if thrown_timeout != timeout:
      raise

    logger.exception('Task timed out')
    # This could probably be calculated, but for now, just retry immediately.
    raise task.retry(countdown=0)
  finally:
    timeout.cancel()


for queue in celery.conf['CELERY_QUEUES']:
  task_function = lambda task, headers, args: execute_task(task, headers, args)
  task_name = get_queue_function_name(queue.name.split('___', 1)[1])
  task_decorator = celery.task(name=task_name, max_retries=10000, bind=True)
  task_decorator(task_function)
