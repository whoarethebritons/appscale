import json
import logging
import os

from .constants import CONFIG_DIR


class VersionManager(object):
  def __init__(self, zk_client):
    self.versions = set()
    self.zk_client = zk_client

  def update_versions(self, revision_keys):
    new_versions = {'_'.join(key.split('_')[:3]) for key in revision_keys}
    # Set up watches for new versions.
    for new_version in new_versions:
      if new_version not in self.versions:
        logging.info('setting up watch for version: {}'.format(new_version))
        self.versions.add(new_version)
        version_node = '/appscale/projects/{}/services/{}/versions/{}'.\
          format(*new_version.split('_'))
        watch_function = lambda znode, _: self.version_watcher(
          znode, new_version)
        self.zk_client.DataWatch(version_node, watch_function)

    to_remove = []
    for existing_version in self.versions:
      if existing_version not in new_versions:
        to_remove.append(existing_version)

    for existing_version in to_remove:
      self.versions.remove(existing_version)

  def version_watcher(self, data, version_key):
    logging.info('new version data: {}'.format(data))
    logging.info('new version key: {}'.format(version_key))
    logging.info('versions: {}'.format(self.versions))
    # Cancel watch if no longer needed.
    if version_key not in self.versions:
      logging.info('version key not in versions')
      return False

    logging.info('version was in versions')
    # If the version has been deleted and it's still assigned, do nothing.
    if not data:
      return

    logging.info('version had data')
    version = json.loads(data)
    http_port = version['appscaleExtensions']['httpPort']
    port_file_location = os.path.join(
      CONFIG_DIR, 'port-{}.txt'.format(version_key))
    with open(port_file_location, 'w') as port_file:
      port_file.write(str(http_port))

    logging.info('{} updated'.format(version_key))
