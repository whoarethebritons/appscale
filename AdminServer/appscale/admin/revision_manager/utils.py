import os

from xml.etree import ElementTree


class InvalidSource(Exception):
  """ Indicates that a revision's source cannot be run. """
  pass


def extract_env_vars_from_xml(xml_file):
  """ Returns any custom environment variables defined in appengine-web.xml.

  Args:
    xml_file: A string containing the location of the xml file.
  Returns:
    A dictionary containing the custom environment variables.
  """
  custom_vars = {}
  tree = ElementTree.parse(xml_file)
  root = tree.getroot()
  for child in root:
    if not child.tag.endswith('env-variables'):
      continue

    for env_var in child:
      var_dict = env_var.attrib
      custom_vars[var_dict['name']] = var_dict['value']

  return custom_vars


def find_web_inf(source_path):
  """ Returns the location of a Java revision's WEB-INF directory.

  Args:
    source_path: A string specifying the location of the revision's source.
  Returns:
    A string specifying the location of the WEB-INF directory.
  Raises:
    BadConfigurationException if the directory is not found.
  """
  # Check for WEB-INF directories that contain the required appengine-web.xml.
  matches = []
  for root, dirs, files in os.walk(source_path):
    if 'appengine-web.xml' in files and root.endswith('/WEB-INF'):
      matches.append(root)

  if not matches:
    raise InvalidSource('Unable to find WEB-INF directory')

  # Use the shortest path.
  shortest_match = matches[0]
  for match in matches:
    match_parts = os.path.split(match)
    shortest_parts = os.path.split(shortest_match)
    if len(match_parts) < len(shortest_parts):
      shortest_match = match
  return shortest_match
