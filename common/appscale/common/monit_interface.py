import logging
import subprocess
import time
import urllib
from xml.etree import ElementTree
from datetime import timedelta

from . import constants
from . import misc

from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop

""" 
This file contains top level functions for starting and stopping 
monitoring of processes using monit. Each component is in
charge of creating configuration files for the process they want started. 
"""

MONIT = "/usr/bin/monit"

NUM_RETRIES = 10

SMALL_WAIT = 3

def run_with_retry(args):
  """ Runs the given monit command, retrying it if it fails (which can occur if
  monit is busy servicing other requests).

  Args:
    args: A list of strs, where each str is a command-line argument composing
      the command to execute.
  Returns:
    True on success, and False otherwise.
  """
  if args[0] != MONIT:
    logging.error("Cannot execute command {0}, as it is not a monit command." \
      .format(args))
    return False

  retries_left = NUM_RETRIES
  while retries_left:
    return_status = subprocess.call(args)
    if return_status == 0:
      logging.info("Monit command {0} returned successfully!".format(args))
      return True

    retries_left -= 1

    logging.warning("Monit command {0} returned with status {1}, {2} retries " \
      "left.".format(args, return_status, retries_left))
    time.sleep(SMALL_WAIT)

  return False

def start(watch, is_group=True):
  """ Instructs monit to start the given program, assuming that a configuration
  file has already been written for it.

  Args:
    watch: A str representing the name of the program to start up and monitor.
    is_group: A bool that indicates if we want to stop a group of programs, or
      only a single program.
  Returns:
    True if the program was started, or False if (1) the named program is not a
    valid program name, (2) if monit could not be reloaded to read the new
    configuration file, or (3) monit could not start the new program.
  """
  if not misc.is_string_secure(watch):
    logging.error("Watch string [{0}] is a possible security violation".format(
      watch))
    return False

  logging.info("Reloading monit.")
  if not run_with_retry([MONIT, 'reload']):
    return False

  logging.info("Starting watch {0}".format(watch))
  if is_group:
    run_with_retry([MONIT, 'monitor', '-g', watch])
    return run_with_retry([MONIT, 'start', '-g', watch])
  else:
    run_with_retry([MONIT, 'monitor',  watch])
    return run_with_retry([MONIT, 'start', watch])

def stop(watch, is_group=True):
  """ Shut down the named programs monit is watching, and stop monitoring it.
 
  Args:
    watch: The name of the group of programs that monit is watching, that should
      no longer be watched.
    is_group: A bool that indicates if we want to stop a group of programs, or
      only a single program.
  Returns:
    True if the named programs were stopped and no longer monitored, and False
    if either (1) the named watch is not valid, (2) the programs could not be
    stopped, or (3) the programs could not be unmonitored.
  """
  if not misc.is_string_secure(watch):
    logging.error("Watch string (%s) is a possible security violation" % watch)
    return False
  
  logging.info("Stopping watch {0}".format(watch))
  if is_group:
    stop_command = [MONIT, 'stop', '-g', watch]
  else:
    stop_command = [MONIT, 'stop', watch]

  return run_with_retry(stop_command)

def restart(watch):
  """ Instructs monit to restart all processes hosting the given watch.

  Args:
    watch: A str representing the name of the programs to restart.
  Returns:
    True if the programs were restarted, or False if (1) the watch is not a
    valid program name, (2) monit could not restart the new program.
  """
  if not misc.is_string_secure(watch):
    logging.error("Watch string [{0}] is a possible security violation".format(
      watch))
    return False

  logging.info("Restarting watch {0}".format(watch))
  return run_with_retry([MONIT, 'restart', '-g', watch])


class ProcessNotFound(Exception):
  pass


def process_status(response, process_name):
  root = ElementTree.XML(response)
  for service in root.iter('service'):
    name = service.find('name').text
    if name != process_name:
      continue

    monitored = int(service.find('monitor').text)
    status = int(service.find('status').text)
    if monitored == 0:
      return constants.MonitStates.UNMONITORED
    elif monitored == 1:
      if status == 0:
        return constants.MonitStates.RUNNING
      else:
        return constants.MonitStates.STOPPED
    else:
      return constants.MonitStates.PENDING

  raise ProcessNotFound('{} not found in monit status'.format(process_name))


class MonitOperator(object):
  def __init__(self, thread_pool):
    self.reload_future = None
    self.thread_pool = thread_pool
    self.client = AsyncHTTPClient()

  @gen.coroutine
  def _reload(self):
    yield gen.sleep(1)
    yield self.thread_pool.submit(subprocess.check_call, ['monit', 'reload'])

  @gen.coroutine
  def reload(self):
    if self.reload_future is None or self.reload_future.done():
      self.reload_future = self._reload()

    yield self.reload_future

  @gen.coroutine
  def get_status(self, process_name):
    status_url = 'http://localhost:2812/_status?format=xml'
    response = yield self.client.fetch(status_url)
    raise gen.Return(process_status(response.body, process_name))

  @gen.coroutine
  def send_start(self, process_name):
    process_url = 'http://localhost:2812/{}'.format(process_name)
    payload = urllib.urlencode({'action': 'start'})
    yield self.client.fetch(process_url, method='POST', body=payload)

  @gen.coroutine
  def wait_for_status(self, process_name):
    while True:
      try:
        status = yield self.get_status(process_name)
        raise gen.Return(status)
      except ProcessNotFound:
        logging.debug('{} not found in monit'.format(process_name))
        yield gen.sleep(.5)

  @gen.coroutine
  def ensure_running(self, process_name):
    while True:
      status_future = self.wait_for_status(process_name)
      status = yield gen.with_timeout(timedelta(seconds=2), status_future,
                                      IOLoop.current())

      if status == constants.MonitStates.RUNNING:
        raise gen.Return()

      if status == constants.MonitStates.UNMONITORED:
        yield self.send_start(process_name)

      yield gen.sleep(1)
