import errno
import fnmatch
import glob
import json
import logging
import os
import random
import shutil
import subprocess
import tarfile

from appscale.common import appscale_info
from appscale.common.constants import (
  APPSCALE_HOME,
  UNPACK_ROOT,
  JAVA,
  GO
)
from appscale.common.appscale_utils import get_md5
from .utils import find_web_inf

from tornado import gen
from kazoo.exceptions import NodeExistsError


class InvalidArchive(Exception):
  pass


def remove_conflicting_jars(source_path):
  """ Removes jars uploaded which may conflict with AppScale jars.
  """
  lib_dir = os.path.join(find_web_inf(source_path), 'lib')
  if not os.path.isdir(lib_dir):
    logging.warn("Lib directory not found in app code while updating.")
    return

  logging.info('Removing jars from {}'.format(lib_dir))
  conflicting_jars_pattern = [
    'appengine-api-1.0-sdk-*.jar',
    'appengine-api-stubs-*.jar',
    'appengine-api-labs-*.jar',
    'appengine-jsr107cache-*.jar',
    'jsr107cache-*.jar',
    'appengine-mapreduce*.jar',
    'appengine-pipeline*.jar',
    'appengine-gcs-client*.jar'
  ]
  for file in os.listdir(lib_dir):
    for pattern in conflicting_jars_pattern:
      if fnmatch.fnmatch(file, pattern):
        os.remove(lib_dir + os.sep + file)


def copy_files_matching_pattern(file_path_pattern, dest):
  """ Copies files matching the specified pattern to the destination directory.
  Args:
      file_path_pattern: The pattern of the files to be copied over.
      dest: The destination directory.
  """
  for file in glob.glob(file_path_pattern):
    shutil.copy(file, dest)


def copy_modified_jars(source_path):
  """ Copies the changes made to the Java SDK
  for AppScale into the apps lib folder.

  Args:
    app_name: The name of the application to run

  Returns:
    False if there were any errors, True if success
  """
  web_inf_dir = find_web_inf(source_path)
  lib_dir = os.path.join(web_inf_dir, 'lib')

  if not os.path.isdir(lib_dir):
    logging.info("Creating lib directory at: {0}".format(lib_dir))
    os.mkdir(lib_dir)

  repacked_lib_dir = os.path.join(
    APPSCALE_HOME, 'AppServer_Java', 'appengine-java-sdk-repacked', 'lib')
  patterns_to_copy = [
    os.path.join(repacked_lib_dir, 'user', '*.jar'),
    os.path.join(repacked_lib_dir, 'impl', 'appscale-*.jar'),
    os.path.join('/', 'usr', 'share', 'appscale', 'ext', '*')
  ]
  for pattern in patterns_to_copy:
    copy_files_matching_pattern(pattern, lib_dir)


def fetch_file(host, location):
  key_file = os.path.join('/', 'etc', 'appscale', 'ssh.key')
  remote_location = '{}:{}'.format(host, location)
  scp_cmd = ['scp', '-i', key_file, remote_location, location]
  subprocess.check_call(scp_cmd)


def canonical_path(path):
  """ Resolves a path, following symlinks.

  Args:
    path: A string specifying a file system location.
  Returns:
    A string specifying a file system location.
  """
  return os.path.realpath(os.path.abspath(path))


def valid_link(link_name, link_target, base):
  """ Checks if a link points to a location that resides within base.

  Args:
    link_name: A string specifying the location of the link.
    link_target: A string specifying the target of the link.
    base: A string specifying the root path of the archive.
  Returns:
    A boolean indicating whether or not the link is valid.
  """
  tip = canonical_path(os.path.join(base, os.path.dirname(link_name)))
  target = canonical_path(os.path.join(tip, link_target))
  return target.startswith(base)


def ensure_path(path):
  """ Ensures directory exists.

  Args:
    path: A string specifying the path to ensure.
  """
  try:
    os.makedirs(os.path.join(path))
  except OSError as os_error:
    if os_error.errno == errno.EEXIST and os.path.isdir(path):
      pass
    else:
      raise


def extract_source(revision_key, location, runtime):
  """ Unpacks an archive to a given location.

  Args:
    revision_key: A string specifying the revision key.
    location: A string specifying the location of the source archive.
    runtime: A string specifying the revision's runtime.
  Raises:
    IOError if version source archive does not exist.
    InvalidArchive if the source archive is not valid.
  """
  logging.info('extracting source')
  revision_base = os.path.join(UNPACK_ROOT, revision_key)
  ensure_path(os.path.join(revision_base, 'log'))

  app_path = os.path.join(revision_base, 'app')
  ensure_path(app_path)
  # The working directory must be the target in order to validate paths.
  os.chdir(app_path)

  with tarfile.open(location, 'r:gz') as archive:
    # Check if the archive is valid before extracting it.
    has_config = False
    for file_info in archive:
      file_name = file_info.name
      if not canonical_path(file_name).startswith(app_path):
        raise InvalidArchive(
          'Invalid location in archive: {}'.format(file_name))

      if file_info.issym() or file_info.islnk():
        if not valid_link(file_name, file_info.linkname, app_path):
          raise InvalidArchive('Invalid link in archive: {}'.format(file_name))

      if runtime == JAVA:
        if file_name.endswith('appengine-web.xml'):
          has_config = True
      else:
        if canonical_path(file_name) == os.path.join(app_path, 'app.yaml'):
          has_config = True

    if not has_config:
      if runtime == JAVA:
        missing_file = 'appengine.web.xml'
      else:
        missing_file = 'app.yaml'
      raise InvalidArchive('Archive must have {}'.format(missing_file))

    archive.extractall(path=app_path)

  if runtime == GO:
    try:
      shutil.move(os.path.join(app_path, 'gopath'), revision_base)
    except IOError:
      logging.debug('{} does not have a gopath directory'.format(revision_key))

  if runtime == JAVA:
    remove_conflicting_jars(app_path)
    copy_modified_jars(app_path)


class SourceManager(object):
  UNKNOWN_LOCATION = 'unknown'

  def __init__(self, zk_client, thread_pool):
    self.zk_client = zk_client
    self.thread_pool = thread_pool
    self.source_futures = {}

  @gen.coroutine
  def fetch_archive(self, revision_key, try_existing=True):
    hosts_with_archive = yield self.thread_pool.submit(
      self.zk_client.get_children, '/apps/{}'.format(revision_key))
    assert hosts_with_archive, '{} has no hosters'.format(revision_key)
    logging.info('hosts with archive: {}'.format(hosts_with_archive))
    host = random.choice(hosts_with_archive)
    host_node = '/apps/{}/{}'.format(revision_key, host)
    archive_details_json, _ = yield self.thread_pool.submit(
      self.zk_client.get, host_node)
    logging.info('archive_details_json: {}'.format(archive_details_json))
    archive_details = json.loads(archive_details_json)
    location = archive_details['location']

    if try_existing and os.path.isfile(location):
      md5 = yield self.thread_pool.submit(get_md5, location)
      if md5 == archive_details['md5']:
        raise gen.Return(archive_details)
      raise InvalidArchive('Source MD5 does not match')

    yield self.thread_pool.submit(fetch_file, host, location)
    md5 = yield self.thread_pool.submit(get_md5, location)
    if md5 != archive_details['md5']:
      raise InvalidArchive('Source MD5 does not match')

    raise gen.Return(archive_details)

  @gen.coroutine
  def prepare_source(self, key, location, runtime):
    try:
      archive_details = yield self.fetch_archive(key)
    except InvalidArchive:
      archive_details = yield self.fetch_archive(key, try_existing=False)

    # Register as a hoster.
    private_ip = appscale_info.get_private_ip()
    new_hoster_node = '/apps/{}/{}'.format(key, private_ip)
    try:
      yield self.thread_pool.submit(self.zk_client.create, new_hoster_node,
                                    json.dumps(archive_details), makepath=True)
    except NodeExistsError:
      logging.debug('{} is already a hoster'.format(private_ip))
    logging.info('added hoster node')
    yield self.thread_pool.submit(extract_source, key, location, runtime)
    logging.info('done preparing source')

  @gen.coroutine
  def ensure_source(self, key, location, runtime):
    """ Wait until the revision source is ready.

    If this method has been previously called for the same revision, it waits
    for the same future. This prevents the same archive from being fetched
    and extracted multiple times.

    Args:
      key: A string specifying the revision key.
      location: A string specifying the location of the source archive.
      runtime: A string specifying the revision's runtime.
    """
    logging.info('ensuring source')
    if key not in self.source_futures:
      self.source_futures[key] = self.prepare_source(key, location, runtime)

    yield self.source_futures[key]
