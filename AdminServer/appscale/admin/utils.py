""" Utility functions used by the AdminServer. """

import json
import os
import socket
import time

from appscale.common.constants import HTTPCodes
from kazoo.exceptions import NoNodeError
from . import constants
from .constants import (
  CustomHTTPError,
  SOURCES_DIRECTORY,
  Types,
  VERSION_PATH_SEPARATOR
)


def assert_fields_in_resource(required_fields, resource_name, resource):
  """ Ensures the resource contains the required fields.

  Args:
    required_fields: An iterable specifying the required fields.
    resource_name: A string specifying the resource name.
    resource: A dictionary containing the resource details.
  Raises:
    CustomHTTPError if there are missing fields.
  """
  def missing_field(prefix, group, resource_part):
    field_name = group.pop(0)
    if field_name not in resource_part:
      return '.'.join([prefix, field_name])

    if not group:
      return

    prefix += '.{}'.format(field_name)
    return missing_field(prefix, group, resource_part[field_name])

  missing_fields = []
  for group in required_fields:
    field = missing_field(resource_name, group.split('.'), resource)
    if field is not None:
      missing_fields.append(field)

  if not missing_fields:
    return

  message = 'The request is invalid.'
  description = 'This field is required.'

  if len(missing_fields) == 1:
    message = '{}: {}'.format(missing_fields[0], description)

  violations = [{'field': field, 'description': description}
                for field in missing_fields]

  raise CustomHTTPError(
    HTTPCodes.BAD_REQUEST,
    message=message,
    status='INVALID_ARGUMENT',
    details=[{'@type': Types.BAD_REQUEST, 'fieldViolations': violations}])


def version_contains_field(version, field):
  """ Checks if the given dictionary contains the given field.

  Args:
    version: A dictionary containing version details.
    field: A string representing a key path.
  Returns:
    A boolean indicating whether or not the version contains the field.
  """
  version_fragment = version
  for field_part in field.split('.'):
    try:
      version_fragment = version_fragment[field_part]
    except KeyError:
      return False

  return True


def apply_mask_to_version(given_version, desired_fields):
  """ Reduces a version to the desired fields.

  Args:
    given_version: A dictionary containing version details.
    desired_fields: A list of strings representing key paths.
  Returns:
    A dictionary containing some version details.
  """
  masked_version = {}
  for field in desired_fields:
    if not version_contains_field(given_version, field):
      continue

    given_version_part = given_version
    masked_version_part = masked_version
    field_parts = field.split('.')
    for index, field_part in enumerate(field_parts):
      if field_part not in masked_version_part:
        if index == (len(field_parts) - 1):
          masked_version_part[field_part] = given_version_part[field_part]
        elif isinstance(given_version_part[field_part], dict):
          masked_version_part[field_part] = {}
        elif isinstance(given_version_part[field_part], list):
          masked_version_part[field_part] = []

      given_version_part = given_version_part[field_part]
      masked_version_part = masked_version_part[field_part]

  return masked_version


def port_is_open(host, port):
  """ Checks if the given port is open.

  Args:
    host: A string specifying the location of the host.
    port: An integer specifying the port to check.
  Returns:
    A boolean indicating whether or not the port is open.
  """
  sock = socket.socket()
  result = sock.connect_ex((host, port))
  return result == 0


def claim_ownership_of_source(project_id, service_id, version):
  """ Renames the given source archive to keep track of it.

  Args:
    project_id: A string specifying a project ID.
    service_id: A string specifying a service ID.
    version: A dictionary containing version details.
  """
  new_filename = VERSION_PATH_SEPARATOR.join(
    [project_id, service_id, version['id'],
     '{}.tar.gz'.format(int(time.time() * 1000))])
  new_location = os.path.join(SOURCES_DIRECTORY, new_filename)
  os.rename(version['deployment']['zip']['sourceUrl'], new_location)
  return new_location


def remove_old_archives(project_id, service_id, version):
  """ Cleans up old revision archives.

  Args:
    project_id: A string specifying a project ID.
    service_id: A string specifying a service ID.
    version: A dictionary containing version details.
  """
  prefix = constants.VERSION_PATH_SEPARATOR.join(
    [project_id, service_id, version['id']])
  current_name = os.path.basename(version['deployment']['zip']['sourceUrl'])
  old_sources = [os.path.join(SOURCES_DIRECTORY, archive) for archive
                 in os.listdir(SOURCES_DIRECTORY)
                 if archive.startswith(prefix) and archive < current_name]
  for archive in old_sources:
    os.remove(archive)


def assigned_locations(zk_client):
  """ Discovers the locations assigned for all existing versions.

  Args:
    zk_client: A KazooClient.
  Returns:
    A set containing used ports.
  """
  try:
    project_nodes = [
      '/appscale/projects/{}'.format(project)
      for project in zk_client.get_children('/appscale/projects')]
  except NoNodeError:
    project_nodes = []

  service_nodes = []
  for project_node in project_nodes:
    project_id = project_node.split('/')[3]
    try:
      new_service_ids = zk_client.get_children(
        '{}/services'.format(project_node))
    except NoNodeError:
      continue
    service_nodes.extend([
      '/appscale/projects/{}/services/{}'.format(project_id, service_id)
      for service_id in new_service_ids])

  version_nodes = []
  for service_node in service_nodes:
    project_id = service_node.split('/')[3]
    service_id = service_node.split('/')[5]
    try:
      new_version_ids = zk_client.get_children(
        '{}/versions'.format(service_node))
    except NoNodeError:
      continue
    version_nodes.extend([
      '/appscale/projects/{}/services/{}/versions/{}'.format(
        project_id, service_id, version_id)
      for version_id in new_version_ids])

  locations = set()
  for version_node in version_nodes:
    try:
      version = json.loads(zk_client.get(version_node)[0])
    except NoNodeError:
      continue

    # Extensions and ports should always be defined when written to a node.
    extensions = version['appscaleExtensions']
    locations.add(extensions['httpPort'])
    locations.add(extensions['httpsPort'])
    locations.add(extensions['haproxyPort'])

  return locations


def assign_ports(old_version, new_version, zk_client):
  """ Assign ports for a version.

  Args:
    old_version: A dictionary containing version details.
    new_version: A dictionary containing version details.
    zk_client: A KazooClient.
  Returns:
    A dictionary specifying the ports to reserve for the version.
  """
  old_extensions = old_version.get('appscaleExtensions', {})
  old_http_port = old_extensions.get('httpPort')
  old_https_port = old_extensions.get('httpsPort')
  haproxy_port = old_extensions.get('haproxyPort')

  new_extensions = new_version.get('appscaleExtensions', {})
  new_http_port = new_extensions.get('httpPort')
  new_https_port = new_extensions.get('httpsPort')

  # If this is not the first revision, and the client did not request
  # particular ports, just use the ports from the last revision.
  if old_http_port is not None and new_http_port is None:
    new_http_port = old_http_port

  if old_https_port is not None and new_https_port is None:
    new_https_port = old_https_port

  # If the ports have not changed, do not check for conflicts.
  if (new_http_port == old_http_port and new_https_port == old_https_port and
      haproxy_port is not None):
    return {'httpPort': new_http_port, 'httpsPort': new_https_port,
            'haproxyPort': haproxy_port}

  taken_locations = assigned_locations(zk_client)

  # If ports were requested, make sure they are available.
  if new_http_port is not None and new_http_port in taken_locations:
    raise CustomHTTPError(HTTPCodes.BAD_REQUEST,
                          message='Requested httpPort is already taken')

  if new_https_port is not None and new_https_port in taken_locations:
    raise CustomHTTPError(HTTPCodes.BAD_REQUEST,
                          message='Requested httpsPort is already taken')

  if new_http_port is None:
    try:
      new_http_port = next(port for port in constants.AUTO_HTTP_PORTS
                           if port not in taken_locations)
    except StopIteration:
      raise CustomHTTPError(HTTPCodes.INTERNAL_ERROR,
                            message='Unable to find HTTP port for version')

  if new_https_port is None:
    try:
      new_https_port = next(port for port in constants.AUTO_HTTPS_PORTS
                            if port not in taken_locations)
    except StopIteration:
      raise CustomHTTPError(HTTPCodes.INTERNAL_ERROR,
                            message='Unable to find HTTPS port for version')

  if haproxy_port is None:
    try:
      haproxy_port = next(port for port in constants.HAPROXY_PORTS
                          if port not in taken_locations)
    except StopIteration:
      raise CustomHTTPError(HTTPCodes.INTERNAL_ERROR,
                            message='Unable to find HAProxy port for version')

  return {'httpPort': new_http_port, 'httpsPort': new_https_port,
          'haproxyPort': haproxy_port}
