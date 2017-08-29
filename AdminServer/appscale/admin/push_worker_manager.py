""" Keeps track of queue configuration details for push workers. """

import logging
import json
import os
from datetime import timedelta

from kazoo.exceptions import ZookeeperError
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.options import options

from appscale.common.constants import (
  LOG_DIR,
  MonitStates,
  PID_DIR,
  CONFIG_DIR
)
from appscale.common.monit_app_configuration import create_config_file
from .utils import ensure_path

# The number of tasks the Celery worker can handle at a time.
CELERY_CONCURRENCY = 1000

# The directory where Celery configuration files are stored.
CELERY_CONFIG_DIR = os.path.join(CONFIG_DIR, 'celery', 'configuration')

# The safe memory in MB per Celery worker.
CELERY_SAFE_MEMORY = 1000

# The directory where Celery persists state.
CELERY_STATE_DIR = os.path.join('/', 'opt', 'appscale', 'celery')

# The working directory for Celery workers.
CELERY_WORKER_DIR = os.path.join(CONFIG_DIR, 'celery', 'workers')

# The directory that workers use for logging.
CELERY_WORKER_LOG_DIR = os.path.join(LOG_DIR, 'celery_workers')

# The time limit of a running task in seconds. Extra time over the soft limit
# allows it to catch up to interrupts.
HARD_TIME_LIMIT = 610

# The soft time limit of a running task.
TASK_SOFT_TIME_LIMIT = 600

# The worker script for Celery to use.
WORKER_MODULE = 'appscale.taskqueue.push_worker'


class ProjectPushWorkerManager(object):
  """ Manages the Celery worker for a single project. """
  def __init__(self, zk_client, monit_operator, project_id):
    """ Creates a new ProjectPushWorkerManager.

    Args:
      zk_client: A KazooClient.
      monit_operator: A MonitOperator.
      project_id: A string specifying a project ID.
    """
    self.zk_client = zk_client
    self.project_id = project_id
    self.monit_operator = monit_operator
    self.queues_node = '/appscale/projects/{}/queues'.format(project_id)
    self.watch = zk_client.DataWatch(self.queues_node, self._update_worker)
    self.monit_watch = 'celery-{}'.format(project_id)
    self._stopped = False

  @gen.coroutine
  def update_worker(self, queue_config):
    """ Updates a worker's configuration and restarts it.

    Args:
      queue_config: A JSON string specifying queue configuration.
    """
    self._write_worker_configuration(queue_config)
    status = yield self._wait_for_stable_state()

    # Start the worker if it doesn't exist. Restart it if it does.
    if status == MonitStates.MISSING:
      command = self.celery_command()
      env_vars = {'APP_ID': self.project_id, 'HOST': options.load_balancers[0],
                  'C_FORCE_ROOT': True}
      pidfile = os.path.join(PID_DIR, 'celery-{}.pid'.format(self.project_id))
      create_config_file(self.monit_watch, command, pidfile, env_vars=env_vars,
                         max_memory=CELERY_SAFE_MEMORY)
      logging.info('Starting push worker for {}'.format(self.project_id))
      yield self.monit_operator.reload()
    else:
      logging.info('Restarting push worker for {}'.format(self.project_id))
      yield self.monit_operator.send_command(self.monit_watch, 'restart')

    start_future = self.monit_operator.ensure_running(self.monit_watch)
    yield gen.with_timeout(timedelta(seconds=60), start_future,
                           IOLoop.current())

  def celery_command(self):
    """ Generates the Celery command for a project's push worker. """
    log_file = os.path.join(CELERY_WORKER_LOG_DIR,
                            '{}.log'.format(self.project_id))
    pidfile = os.path.join(PID_DIR, 'celery-{}.pid'.format(self.project_id))
    state_db = os.path.join(CELERY_STATE_DIR,
                            'worker___{}.db'.format(self.project_id))

    return ' '.join([
      'celery', 'worker',
      '--app', WORKER_MODULE,
      '--pool=eventlet',
      '--concurrency={}'.format(CELERY_CONCURRENCY),
      '--hostname', self.project_id,
      '--workdir', CELERY_WORKER_DIR,
      '--logfile', log_file,
      '--pidfile', pidfile,
      '--time-limit', str(HARD_TIME_LIMIT),
      '--soft-time-limit', str(TASK_SOFT_TIME_LIMIT),
      '--statedb', state_db,
      '-Ofair'
    ])

  def ensure_watch(self):
    """ Restart the watch if it has been cancelled. """
    if self._stopped:
      self._stopped = False
      self.watch = self.zk_client.DataWatch(self.queues_node,
                                            self._update_worker)

  @gen.coroutine
  def _wait_for_stable_state(self):
    """ Waits until the worker's state is not pending. """
    stable_states = (MonitStates.MISSING, MonitStates.RUNNING,
                     MonitStates.UNMONITORED)
    status_future = self.monit_operator.wait_for_status(
      self.monit_watch, stable_states)
    status = yield gen.with_timeout(timedelta(seconds=60), status_future,
                                    IOLoop.current())
    raise gen.Return(status)

  def _write_worker_configuration(self, queue_config):
    """ Writes a worker's configuration file.

    Args:
      queue_config: A JSON string specifying queue configuration.
    """
    if queue_config is None:
      rates = {'default': '5/s'}
    else:
      queues = json.loads(queue_config)['queue']
      rates = {
        queue_name: queue['rate'] for queue_name, queue in queues.items()
        if 'mode' not in queue or queue['mode'] == 'push'}

    config_location = os.path.join(CELERY_CONFIG_DIR,
                                   '{}.json'.format(self.project_id))
    with open(config_location, 'w') as config_file:
      json.dump(rates, config_file)

  def _update_worker(self, queue_config, _):
    """ Handles updates to a queue configuration node.

    Since this runs in a separate thread, it doesn't change any state directly.
    Instead, it just acts as a bridge back to the main IO loop.

    Args:
      queue_config: A JSON string specifying queue configuration.
    """
    main_io_loop = IOLoop.instance()

    # Prevent further watches if they are no longer needed.
    if queue_config is None:
      try:
        project_exists = self.zk_client.exists(
          '/appscale/projects/{}'.format(self.project_id)) is not None
      except ZookeeperError:
        # If the project has been deleted, an extra "exists" watch will remain.
        project_exists = True

      if not project_exists:
        self._stopped = True
        return False

    main_io_loop.add_callback(self.update_worker, queue_config)


class GlobalPushWorkerManager(object):
  """ Manages the Celery workers for all projects. """
  def __init__(self, zk_client, monit_operator):
    """ Creates a new GlobalPushWorkerManager. """
    self.zk_client = zk_client
    self.monit_operator = monit_operator
    self.projects = {}
    ensure_path(CELERY_CONFIG_DIR)
    ensure_path(CELERY_WORKER_DIR)
    ensure_path(CELERY_WORKER_LOG_DIR)
    ensure_path(CELERY_STATE_DIR)
    zk_client.ensure_path('/appscale/projects')
    zk_client.ChildrenWatch('/appscale/projects', self._update_projects)

  def update_projects(self, new_project_list):
    """ Establishes watches for each project's queue configuration.

    Args:
      new_project_list: A fresh list of strings specifying existing
        project IDs.
    """
    to_stop = [project for project in self.projects
               if project not in new_project_list]
    for project_id in to_stop:
      del self.projects[project_id]

    for new_project_id in new_project_list:
      if new_project_id not in self.projects:
        self.projects[new_project_id] = ProjectPushWorkerManager(
          self.zk_client, self.monit_operator, new_project_id)

      # Handle changes that happen between watches.
      self.projects[new_project_id].ensure_watch()

  def _update_projects(self, new_projects):
    """ Handles creation and deletion of projects.

    Since this runs in a separate thread, it doesn't change any state directly.
    Instead, it just acts as a bridge back to the main IO loop.

    Args:
      new_projects: A list of strings specifying all existing project IDs.
    """
    main_io_loop = IOLoop.instance()
    main_io_loop.add_callback(self.update_projects, new_projects)
