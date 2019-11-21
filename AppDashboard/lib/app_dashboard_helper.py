# pylint: disable-msg=W0703
# pylint: disable-msg=R0201

import datetime
import hashlib
import json
import logging
import os
import re
import SOAPpy
import tempfile
import time
import urllib

from appscale.appcontroller_client import AppControllerClient
from google.appengine.api import urlfetch
from google.appengine.api import users

from admin_server_location import ADMIN_SERVER_LOCATION
from controller_location import CONTROLLER_LOCATION
from custom_exceptions import BadConfigurationException
from local_state import LocalState
from secret_key import GLOBAL_SECRET_KEY
from uaserver_location import UA_SERVER_LOCATION


class AppHelperException(Exception):
  """ A special Exception class that should be thrown if a SOAP call to the
  AppController or UserAppServer failed, or returned malformed data. """
  pass


class AppUploadStatuses(object):
  """ A class containing the possible values that the AppController can return
  when checking the status of an upload.
  """
  ID_NOT_FOUND = 'Reservation ID not found.'
  STARTING = 'starting'
  COMPLETE = 'true'


class AppDashboardHelper(object):
  """ Helper class that interacts with the AppController and UserAppServer on
  behalf of the AppDashboard.

  Specifically, the AppController has information about each server running in
  this AppScale deployment (e.g., CPU, memory, disk usage), and the
  UserAppServer has information about the user accounts registered, their
  permissions, and any Google App Engine applications that are running.
  """

  # A str that indicates the name of the cookie that the AppDashboard reads and
  # writes a user's information (their e-mail address, nickname, and list of
  # applications they own) to.
  DEV_APPSERVER_LOGIN_COOKIE = 'dev_appserver_login'

  # A str that separates the four fields stored in the login cookie.
  LOGIN_COOKIE_FIELD_SEPARATOR = ':'

  # A str that separates apps in the app owner list field in the login cookie.
  LOGIN_COOKIE_APPS_SEPARATOR = ','

  # An int indicating which position (starting at zero) the app owner list is in
  # the login cookie.
  LOGIN_COOKIE_APPS_PART = 2

  # The port that the AdminServer runs on.
  ADMIN_SERVER_PORT = 17441

  # The default service for a project.
  DEFAULT_SERVICE = 'default'

  # The default version for a service.
  DEFAULT_VERSION = 'v1'

  # The character used to separate portions of a complete version string.
  # (e.g. guestbook_default_v1)
  VERSION_PATH_SEPARATOR = '_'

  # Users have a list of applications that they own stored in their user data.
  # This character is the delimiter that separates them in their data.
  APP_DELIMITER = ":"

  # When querying the UserAppServer for a list of all the users that are
  # registered in the system, this character is used to separate them.
  USER_DELIMITER = ":"

  # Users have a list of authorizations (capabilities) that correspond to
  # actions they are allowed to perform in this AppScale deployment. The
  # UserAppServer joins that list with this character.
  USER_CAPABILITIES_DELIMITER = ':'

  # A regular expression that can be used to find out which Google App Engine
  # applications a user owns, when applied to their user data.
  USER_APP_LIST_REGEX = "\napplications:(.+)\n"

  # Indicates that the user is a cloud-level administrator.
  CLOUD_ADMIN_MARKER = 'CLOUD_ADMIN'

  # A regular expression that can be used to find out from the user's data in
  # the UserAppServer if they are a cloud-level administrator in this AppScale
  # cloud.
  CLOUD_ADMIN_REGEX = "is_cloud_admin:true"

  # A regular expression that can be used to get the user's nickname (everything
  # preceding the initial '@' symbol) from their e-mail address.
  USERNAME_FROM_EMAIL_REGEX = '\A(.*)@'

  # A regular expression that can be used to retrieve the SHA1-hashed password
  # stored in a user's data with the UserAppServer.
  USER_DATA_PASSWORD_REGEX = 'password:([0-9a-fA-F]+)'

  # A regular expression that can be used to see if the given user is actually
  # a valid user in our system. This is useful in cases when the UserAppServer
  # returns error messages instead of user names.
  ALL_USERS_NON_USER_REGEX = '^[_]+$'

  # The date and time that user tokens expire.
  # TODO: Since this value corresponds to a date in the past, investigate
  # whether or not we still need these tokens, and remove them if we don't.
  TOKEN_EXPIRATION = "20121231120000"

  # Indicates whether or not to use Shibboleth for authentication.
  # Note: If you decide to use Shibboleth, make sure to modify firewall.conf
  # to only allow connections to the dashboard from the Shibboleth connector.
  USE_SHIBBOLETH = False

  # The full url of the Shibboleth connector.
  # This is only applicable if USE_SHIBBOLETH is True.
  SHIBBOLETH_CONNECTOR = ''

  # The domain to use when setting the AppServer cookie.
  # This is only applicable if USE_SHIBBOLETH is True.
  SHIBBOLETH_COOKIE_DOMAIN = 'appscale.com'

  # The port that the Shibboleth connector is listening on.
  SHIBBOLETH_CONNECTOR_PORT = '443'

  # The URL to redirect to upon logging out. This is often needed to instruct
  # the user to close their browser in order to clear the cookie set by the
  # shibboleth IdP.
  SHIBBOLETH_LOGOUT_URL = SHIBBOLETH_CONNECTOR + '/Shibboleth.sso/Logout'

  # The time in seconds to wait before re-checking the app upload status.
  APP_UPLOAD_CHECK_INTERVAL = 1

  # The sentinel app name that indicates that no apps are running on a given
  # machine.
  NO_APPS_RUNNING = "none"

  def __init__(self):
    """ Sets up SOAP client fields, to avoid creating a new SOAP connection for
    every SOAP call.

    Fields:
      appcontroller: A AppControllerClient, which is a SOAP client connected to
        the AppController running on this machine, responsible for service
        deployment and configuration.
      uaserver: A SOAP client connected to the UserAppServer running in this
        AppScale deployment, responsible for managing user and application
        creation.
      cache: A dict that will store the results of SOAP calls made to the
        AppController or UserAppServer, used to avoid making repeated SOAP calls
        for the same data.
    """
    self.appcontroller = None
    self.uaserver = None
    self.cache = {
      'get_role_info': [],
      'query_user_data': {},
      'user_caps': {}
    }

  def get_appcontroller_client(self, server_ip=CONTROLLER_LOCATION):
    """ Retrieves our saved AppController connection, creating a new one if none
    currently exist.

    Args:
      server_ip: An IP address specifying which machine to make AppController
        calls to.
    Returns:
      An AppControllerClient, representing a connection to the AppController.
    """
    if self.appcontroller is None:
      self.appcontroller = AppControllerClient(server_ip, GLOBAL_SECRET_KEY)
    return self.appcontroller

  def get_uaserver(self):
    """ Retrieves our saved UserAppServer connection, creating a new one if none
    currently exist.

    Returns:
      An SOAPpy object, representing a connection to the UserAppServer.
    """
    if self.uaserver is None:
      self.uaserver = SOAPpy.SOAPProxy('http://{}'.format(UA_SERVER_LOCATION))

    return self.uaserver

  def get_user_capabilities(self, email):
    """ Queries the UserAppServer to learn what actions the named user is
    authorized to perform in this AppScale deployment.

    Args:
      email: A str containing the email of the user whose authorizations we want
        to retrieve.
    Returns:
      A list, where each item is a str corresponding to an action this user is
      authorized to perform in this AppScale deployment.
    """
    if email in self.cache['user_caps']:
      return self.cache['user_caps'][email]

    try:
      capabilities = self.get_uaserver().get_capabilities(email,
        GLOBAL_SECRET_KEY).split(self.USER_CAPABILITIES_DELIMITER)
      self.cache['user_caps'][email] = capabilities
      return capabilities
    except Exception as err:
      logging.exception(err)
      return []

  def get_status_info(self):
    """ Queries our local AppController to get server-level information about
    every server running in this AppScale deployment.

    Returns:
      A list of dicts, where each dict contains VM-level info (e.g., CPU,
      memory, disk usage) about that machine. The empty list is returned if
      there was a problem retrieving this information.
    """
    try:
      nodes = self.get_appcontroller_client().get_cluster_stats()
      statuses = []
      for node in nodes:
        cpu_usage = node['cpu']['percent']
        total_memory = node['memory']['available'] + node['memory']['used']
        memory_usage = round(100.0 * node['memory']['used'] /
                             total_memory, 1)
        disks = []
        for mountpoint, disk_info in node['partitions_dict'].iteritems():
          disks.append({'mountpoint': mountpoint,
                        'percentage': round(100.0 * disk_info['used'] / disk_info['total'], 1)})

        statuses.append({'ip': node['public_ip'], 'cpu': str(cpu_usage),
                         'memory': str(memory_usage), 'disk': disks,
                         'roles': node['roles'],
                         'key': str(node['public_ip']).translate(None, '.')})
      return statuses
    except Exception as err:
      logging.exception(err)
      return []

  def get_instance_info(self, app_id):
    """ Queries the AppController to get instance information for a given app_id

    Returns:
      A list of dicts containing host, port, and language information for
        each instance hosting the given application.
    """
    version_key = '_'.join([app_id, self.DEFAULT_SERVICE,
                            self.DEFAULT_VERSION])
    try:
      instances = self.get_appcontroller_client().get_instance_info()
      instance_infos = [{
                          'host': instance.get('host'),
                          'port': instance.get('port'),
                          'language': instance.get('language')
                        } for instance in instances\
                        if instance.get('versionKey') == version_key]
      return instance_infos
    except Exception as err:
      logging.exception(err)

  def get_version_info(self):
    """ Queries the AppController for information about active versions.

    Returns:
      A dictionary mapping version keys to serving URLs. A None value indicates
      that the version is still loading.
    """
    try:
      status_on_all_nodes = self.get_appcontroller_client().get_cluster_stats()
      version_names_and_urls = {}

      if not status_on_all_nodes:
        return {}

      for status in status_on_all_nodes:
        for version_key, done_loading in status['apps'].iteritems():
          if version_key == self.NO_APPS_RUNNING:
            continue
          if done_loading:
            try:
              host_url = self.get_login_ip()
              ports = self.get_version_ports(version_key)
              version_names_and_urls[version_key] = [
                "http://{0}:{1}".format(host_url, ports[0]),
                "https://{0}:{1}".format(host_url, ports[1])]
            except AppHelperException:
              version_names_and_urls[version_key] = None
          else:
            version_names_and_urls[version_key] = None
      return version_names_and_urls
    except Exception as err:
      logging.exception(err)
      return {}

  def get_application_cron_info(self, app_name):
    """ Get an application cron info

    Args:
      app_name: A str containing the name of the app to be removed.
    Returns:
      A dict that contains the cron.yaml and /etc/cron.d/appscale-#app_id files content
    """
    try:
      acc = self.get_appcontroller_client()
      cron_info = acc.get_application_cron_info(app_name)
    except Exception as err:
      logging.exception(err)
      return {}
    return cron_info

  def get_host_with_role(self, role):
    """ Queries the AppController to find a host running the named role.

    Args:
      role: A str indicating the name of the role we wish to find a hoster of.
    Returns:
      A str containing the publicly accessible hostname (IP address or FQDN)
      of one machine that runs the specified service. Note that if multiple
      services host the named role, only one is returned, and if information
      about the named role couldn't be found, the empty string is returned.
    """
    acc = self.get_appcontroller_client()
    if self.cache['get_role_info']:
      nodes = self.cache['get_role_info']
    else:
      try:
        nodes = acc.get_role_info()
        self.cache['get_role_info'] = nodes
      except Exception as err:
        logging.exception(err)
        return ''
    for node in nodes:
      if role in node['roles']:
        return node['public_ip']
    return ''

  def get_head_node_ip(self):
    """ Queries the AppController to learn which machine runs the shadow
    service in this AppScale deployment.

    Returns:
      A str containing the hostname (an IP address or FQDN) of the machine
      running the shadow service.
    """
    return self.get_host_with_role('shadow')

  def get_login_ip(self):
    """ Queries the AppController to learn the public IP of this
    deployment.

    Returns:
      A str containing the hostname (an IP address or FQDN) of the machine
      running the login service.
    """
    login_property = ''
    acc = self.get_appcontroller_client()
    try:
      login_property = acc.get_property('login')
    except Exception as err:
      logging.exception(err)
      return ''
    return login_property.get('login')

  def get_version_ports(self, version_key):
    """ Queries the UserAppServer to learn which port the named application runs
    on.

    Note that we don't need to query the UserAppServer to learn which host the
    application runs on, as it is always full proxied by the machine running the
    login service.

    Args:
      version_key: A str that indicates which version we want to find a hosted
        port for.
    Returns:
      A list that indicates which ports the version runs on. ex. [8080,1443]
    Raises:
      AppHelperException: If the version is not running in this
        AppScale deployment, or if it is running but does not have a port
        assigned to it.
    """
    project_id, service_id, version_id = version_key.split(
      self.VERSION_PATH_SEPARATOR)
    admin_server = 'https://{}:{}'.format(ADMIN_SERVER_LOCATION,
                                          self.ADMIN_SERVER_PORT)
    version_url = '{}/v1/apps/{}/services/{}/versions/{}'.format(
      admin_server, project_id, service_id, version_id)
    result = urlfetch.fetch(version_url,
                            headers={'AppScale-Secret': GLOBAL_SECRET_KEY},
                            validate_certificate = False)

    if result.status_code != 200:
      raise AppHelperException(result.content)

    try:
      version_details = json.loads(result.content)
    except ValueError:
      raise AppHelperException('Invalid response: {}'.format(result.content))

    extensions = version_details['appscaleExtensions']
    return extensions['httpPort'], extensions['httpsPort']

  def shell_check(self, argument):
    """ Checks for special characters in arguments that are part of shell
    commands.

    Args:
      argument: A str, the argument to be checked.
    Raises:
      BadConfigurationException if single quotes are present in argument.
    """
    if '\'' in argument:
      raise BadConfigurationException("Single quotes (') are not allowed " + \
                                      "in filenames.")

  def upload_app(self, filename, upload_file):
    """ Uploads an Google App Engine application into this AppScale deployment.

    Args:
      filename: The name of the file that the user uploaded (used so that the
        tempfile we write has the same extension).
      upload_file: A file object containing the uploaded file's data.
    Returns:
      A str indicating that the application was uploaded successfully.
    Raises:
      AppHelperException: If the application was not uploaded successfully.
    """
    user = users.get_current_user()
    if not user:
      raise AppHelperException("There was an error uploading your "
                               "application. You must be logged in to upload "
                               "applications.")
    try:
      self.shell_check(filename)
      file_suffix = re.search("\.(.*)\Z", filename).group(1)

      # The local controller needs to SCP the tempfile to the shadow node.
      acc = self.get_appcontroller_client(server_ip='127.0.0.1')

      # The sandboxed version of tempfile does not support specifying a suffix.
      with tempfile.NamedTemporaryFile(delete=False) as tgz_file:
        tgz_file.write(upload_file.read())

      try:
        upload_info = acc.upload_app(tgz_file.name, file_suffix)
        status = upload_info['status']

        while status == AppUploadStatuses.STARTING:
          time.sleep(self.APP_UPLOAD_CHECK_INTERVAL)
          status = acc.get_app_upload_status(upload_info['reservation_id'])
          if status == AppUploadStatuses.ID_NOT_FOUND:
            raise AppHelperException(
              'We could not find the reservation ID for your app. '
              'Please try uploading it again.')
          if status == AppUploadStatuses.COMPLETE:
            return 'Application uploaded successfully. Please wait for the ' \
                   'application to start running.'
        raise AppHelperException(
          'Unknown app upload status: {}'.format(status))
      finally:
        os.remove(tgz_file.name)

    except Exception as err:
      logging.exception(err)

      # Only give the user the first line of the exception, since it tells them
      # exactly what the problem with their app is.
      # We use this odd-looking regex to parse out whatever is between the 'red'
      # characters that termcolor emits as the error.
      match_data = re.search("\[31m(.*)\x1b", str(err))
      if match_data:
        failure_message = match_data.group(1)
      else:
        # Fall back to whatever the exception was if it wasn't in the expected
        # format.
        failure_message = str(err)
      raise AppHelperException("There was an error uploading your application: "
                               "{0}".format(failure_message))

  def relocate_version(self, version_key, http_port, https_port):
    """ Relocates a version to different ports.

      Args:
        version_key: A string specifying the version to be relocated
        http_port: The HTTP Port to relocate the application to
        https_port: The HTTPS Port to relocate the application to
      Returns:
        A str indicating that the application was relocated successfully.
      Raises:
        AppHelperException: If the application was not relocated successfully.
      """
    acc = self.get_appcontroller_client()
    try:
      relocate_info = acc.relocate_version(version_key, http_port, https_port)
      # Returns:
      # "OK" if the relocation occurred successfully, and a String containing
      # the reason why the relocation failed in all other cases.
      if relocate_info != "OK":
        logging.error("AppController returned: {0}".format(relocate_info))
        return "Error attempting to relocate Application: {0}" \
          .format(relocate_info)
    except Exception as err:
      logging.exception(err)
      return "There was an error attempting to relocate the application."
    return "Application was relocated successfully."

  def delete_app(self, appname):
    """ Removes a Google App Engine application from this AppScale deployment.

    Args:
      appname: A str containing the name of the app to be removed.
    Returns:
      A str indicating whether or not the application was successfully removed
        from this AppScale deployment.
    """
    admin_server = 'https://{}:{}'.format(ADMIN_SERVER_LOCATION,
                                          self.ADMIN_SERVER_PORT)
    version_url = '{}/v1/apps/{}/services/{}/versions/{}'.format(
      admin_server, appname, self.DEFAULT_SERVICE, self.DEFAULT_VERSION)
    result = urlfetch.fetch(version_url,
                            method=urlfetch.DELETE,
                            headers={'AppScale-Secret': GLOBAL_SECRET_KEY},
                            validate_certificate=False)
    if result.status_code != 200:
      return result.content

    return "Application removed successfully. Please wait for your app to " + \
           "shut down."

  def is_user_logged_in(self):
    """ Checks to see if this user is logged in.

    Returns:
      True if the user is logged in, and False otherwise.
    """
    return users.get_current_user() is not None

  def get_user_email(self):
    """ Get the logged in user's email.

    Returns:
      A str with the user's email, or '' if the user is not logged in.
    """
    user = users.get_current_user()
    if user:
      return user.email()
    else:
      return ''

  def get_owned_apps(self, email=None):
    """ Queries the UserAppServer to see which application ids the named user
    is an administrator on.

    Args:
      email: A str indicating the e-mail address of the user whose data we we
        wish to query. If None is provided instead of a str, then we use the
        currently logged-in user.
    Returns:
      A list of strs, where each str represents an appid that this user owns.
      If no user is logged in, and the caller wants to use the logged-in user's
      email address, the empty list is returned.
    """
    if email is None:
      user = users.get_current_user()
      if not user:
        return []
      email = user.email()
    user_data = self.query_user_data(email)
    user_data_match = re.search(self.USER_APP_LIST_REGEX, user_data)
    if user_data_match:
      return user_data_match.group(1).split(self.APP_DELIMITER)
    return []

  def query_user_data(self, email):
    """ Searches through our cache or queries the UserAppServer for the data it
    stores for the given user.

    Args:
      email: A str that contains the e-mail address for the user whose
        information we want to retrieve.
    Returns:
      A str containing the user's data, or the empty string if their data could
      not be retrieved.
    """
    if email in self.cache['query_user_data']:
      return self.cache['query_user_data'][email]

    try:
      user_data = self.get_uaserver().get_user_data(email, GLOBAL_SECRET_KEY)
      self.cache['query_user_data'][email] = user_data
      return user_data
    except Exception as err:
      logging.exception(err)
      return ''

  def is_user_cloud_admin(self, email=None):
    """ Checks if a user is a cloud administrator.

    Args:
      email: A str containing the e-mail address of the user that may be a cloud
        admin, or None (in which case, we use the e-mail address of the
        currently logged-in user).
    Returns:
      True if the user is a cloud admin, and False otherwise (including the case
      when no user is logged in).
    """
    if email is None:
      user = users.get_current_user()
      if not user:
        return False
      email = user.email()
    user_data = self.query_user_data(email)
    if re.search(self.CLOUD_ADMIN_REGEX, user_data):
      return True
    else:
      return False

  def can_upload_apps(self, email=None):
    """ Checks if the user can upload Google App Engine applications via the
    AppDashboard.

    Args:
      email: A str containing the e-mail address of the user that may be a cloud
        admin, or None (in which case, we use the e-mail address of the
        currently logged-in user).
    Returns:
      True if the user is authorized to upload Google App Engine apps, and False
      otherwise (including the case when no user is logged in).
    """
    if email is None:
      user = users.get_current_user()
      if not user:
        return False
      email = user.email()
    return 'upload_app' in self.get_user_capabilities(email)

  def create_new_user(self, email, password, response,
                      account_type='xmpp_user'):
    """ Creates a new user account, by making both a standard login and an
    XMPP login account.

    Args:
      email: A str containing the e-mail address of the new user.
      password: A str containing the cleartext password for the new user.
      response: A webapp2 response that the new user's logged in cookie
        should be set in.
    Returns:
      True, if the user account was successfully created.
    Raises:
      AppHelperException: If the user account could not be created.
    """
    try:
      uaserver = self.get_uaserver()
      # First, create the standard account.
      encrypted_pass = LocalState.encrypt_password(email, password)
      result = uaserver.commit_new_user(email, encrypted_pass, account_type,
                                        GLOBAL_SECRET_KEY)
      if result != 'true':
        raise AppHelperException(result)

      # Next, create the XMPP account. if the user's e-mail is a@a.a, then that
      # means their XMPP account name is a@login_ip.
      username_regex = re.compile(self.USERNAME_FROM_EMAIL_REGEX)
      username = username_regex.match(email).groups()[0]
      xmpp_user = "{0}@{1}".format(username,
                                   self.get_login_ip())
      xmpp_pass = LocalState.encrypt_password(xmpp_user, password)
      result = uaserver.commit_new_user(xmpp_user, xmpp_pass, account_type,
                                        GLOBAL_SECRET_KEY)
      if result != 'true':
        raise AppHelperException(result)

      # TODO: We may not even be using this token since the switch to
      # full proxy nginx. Investigate this.
      self.create_token(email, email)
      self.set_appserver_cookie(email, self.get_user_app_list(email), response)
    except AppHelperException as err:
      logging.exception(err)
      raise AppHelperException(str(err))
    except Exception as err:
      logging.exception(err)
      raise AppHelperException(str(err))
    return True

  def get_user_app_list(self, email):
    """ Queries the UserAppServer to retrieve a list of apps that the
    user is an admin of.

    Args:
      email: A str containing the e-mail address of the user who we should
        login as.
    Returns:
      A list of strs, each the name of an app the user is an admin of.
    """
    user_data = self.query_user_data(email)
    app_re = re.search(self.USER_APP_LIST_REGEX, user_data)
    if app_re:
      apps_list = app_re.group(1).split(self.APP_DELIMITER)
      return apps_list
    return []

  def set_appserver_cookie(self, email, apps_list, response):
    """ Creates a new cookie indicating that this user is logged in and sets it
    in their session.

    Args:
      email: A str containing the e-mail address of the user who we should
        login as.
      apps_list: A list of strs, each the name of an app the user is an admin
        of.
      response: A webapp2 response that the new user's logged in cookie
        should be set in.
    """
    # Add an extra value to indicate that cloud admins have access to all apps.
    full_admin_list = apps_list
    if self.is_user_cloud_admin(email):
      full_admin_list.append(self.CLOUD_ADMIN_MARKER)

    apps = self.LOGIN_COOKIE_APPS_SEPARATOR.join(full_admin_list)
    if AppDashboardHelper.USE_SHIBBOLETH:
      response.set_cookie(self.DEV_APPSERVER_LOGIN_COOKIE,
                          value=self.get_cookie_value(email, apps),
                          domain=AppDashboardHelper.SHIBBOLETH_COOKIE_DOMAIN,
                          expires=datetime.datetime.now() + datetime.timedelta(
                            days=1))
    else:
      response.set_cookie(self.DEV_APPSERVER_LOGIN_COOKIE,
                          value=self.get_cookie_value(email, apps),
                          expires=datetime.datetime.now() + datetime.timedelta(
                            days=1))

  def get_cookie_app_list(self, request):
    """ Look at the user's login cookie and return the list of apps that
    they are an owner of.

    The login cookie's value has the form: "email:nick:apps:hash".  The email
    is the login email of the user, the nick is the assigned nickname for the
    user, the apps is a comma seperate list of app that this user is an owner
    of, and the hash is a security hash of the first three parts and the
    secret key of the deployment.

    Args:
      request: A webapp2 request that contains the user's login cookie.
    Returns:
      A list of strs, each the name of an app the user is an admin of.
    """
    if self.DEV_APPSERVER_LOGIN_COOKIE in request.cookies:
      cookie_value = urllib.unquote(
        request.cookies[self.DEV_APPSERVER_LOGIN_COOKIE])
      if cookie_value:
        cookie_parts = cookie_value.split(self.LOGIN_COOKIE_FIELD_SEPARATOR)
        if len(cookie_parts) > self.LOGIN_COOKIE_APPS_PART:
          return cookie_parts[self.LOGIN_COOKIE_APPS_PART].split(
            self.LOGIN_COOKIE_APPS_SEPARATOR)
    return []

  def update_cookie_app_list(self, owned_apps, request, response):
    """ Update the login cookie with the list of apps the user is an admin of.

    Look at the user's login cookie and compare the list of apps that they are
    an owner of to the list of apps passed in. The owned_apps parameter is
    considered authoritative, and will overwrite the cookie values if they
    differ.

    Args:
      owned_apps: A list of strs, each the name of an app the user is an admin
        of.
      request: A webapp2 request object that contains the user's login cookie.
      response: A webapp2 response object this is used to set the user's update
        login cookie.
    Returns:
      True if an updated cookie was set, otherwise False.
    """
    user = users.get_current_user()
    if not user:
      return
    email = user.email()
    cookie_apps = self.get_cookie_app_list(request)
    if set(owned_apps) != set(cookie_apps):
      self.set_appserver_cookie(email, owned_apps, response)
      return True
    else:
      return False

  def get_cookie_value(self, email, apps):
    """ Generates a hash corresponding to the given user's credentials.

    It is a hashed string containing the email, nickname, and list of apps the
    user is an admin of. We hash this information with the secret key (not known
    to the user) to prevent users from tampering with their cookie to alter
    who they are logged in as or what apps they own.

    Args:
      email: A str containing the e-mail address of the user to generate a
        cookie value for.
      apps: A list of strs, where each str is an application id that the user is
        an administrator of.
    Retuns:
      A str that is the value of the login cookie.
    """
    nick = re.search('^(.*)@', email).group(1)
    hsh = self.get_appengine_hash(email, nick, apps)
    return urllib.quote("{1}{0}{2}{0}{3}{0}{4}".format(
      self.LOGIN_COOKIE_FIELD_SEPARATOR, email, nick, apps, hsh))

  def get_appengine_hash(self, email, nick, apps):
    """ Generates a hash of the user's credentials with the secret key, used to
    ensure that the user doesn't forge their cookie (as its value would fail to
    match this hash).

    Args:
      email: A str containing the e-mail address of the user to create a hash
        for.
      nick: The prefix of the user's e-mail address (everything before the
        initial '@' character).
      apps: A str with a comma-separated list of apps that this user is
        authorized to administer.
    Returns:
      A str that is the SHA1 hash of the input values with the secret key.
    """
    return hashlib.sha1("{0}{1}{2}{3}".format(email, nick, apps,
                                              GLOBAL_SECRET_KEY)).hexdigest()

  def create_token(self, token, email):
    """ Create a login token and save it in the UserAppServer.

    Args:
      token: A str containing the name of the token to create (usually the email
        address).
      email: A str containing the e-mail address of the user to create the login
        token for.
    """
    try:
      uaserver = self.get_uaserver()
      uaserver.commit_new_token(token, email, self.TOKEN_EXPIRATION,
                                GLOBAL_SECRET_KEY)
    except Exception as err:
      logging.exception(err)

  def logout_user(self, response):
    """ Remove the user's login cookie and invalidate the login token in
      the AppScale deployment. This results in the user being logged out.

      If the user is already logged out, nothing happens.

    Args:
      response: A webapp2 response that the user's logged in cookie should be
        erased from.
    """
    user = users.get_current_user()
    if user:
      self.create_token('invalid', user.email())
      if AppDashboardHelper.USE_SHIBBOLETH:
        response.delete_cookie(self.DEV_APPSERVER_LOGIN_COOKIE,
                               domain=AppDashboardHelper.SHIBBOLETH_COOKIE_DOMAIN)
      else:
        response.delete_cookie(self.DEV_APPSERVER_LOGIN_COOKIE)

  def login_user(self, email, password, response):
    """ Checks to see if the user has entered in a valid email and password,
    logging the user in if they have.

    Args:
      email: A str containing the e-mail address of the user to login.
      password: A str containing the cleartext password of the user to login.
      response: A webapp2 response that the new user's logged in cookie
        should be set in.
    Return:
      True if the user logged in successfully, and False otherwise.
    """
    user_data = self.query_user_data(email)
    server_re = re.search(self.USER_DATA_PASSWORD_REGEX, user_data)
    if not server_re:
      logging.error("Failed Login: {0} regex failed".format(email))
      return False
    server_pwd = server_re.group(1)
    encrypted_pass = LocalState.encrypt_password(email, password)
    if server_pwd != encrypted_pass:
      logging.info("Failed Login: {0} password mismatch".format(email))
      return False
    self.create_token(email, email)
    self.set_appserver_cookie(email, self.get_user_app_list(email), response)
    return True

  def list_all_users(self):
    """ Queries the UserAppServer and return a list of all users in the system.

    Returns:
      A list of strings, where each string is a user's e-mail address.
    """
    ret_list = []
    try:
      uas = self.get_uaserver()
      all_users = uas.get_all_users(GLOBAL_SECRET_KEY)
      all_users_list = all_users.split(self.USER_DELIMITER)
      my_ip = self.get_head_node_ip()
      for usr in all_users_list:
        if re.search('@' + my_ip + '$', usr):  # Skip the XMPP user accounts.
          continue
        if re.search(self.ALL_USERS_NON_USER_REGEX, usr):  # Skip non users.
          continue
        ret_list.append(usr)
    except Exception as err:
      logging.exception(err)
    return ret_list

  def list_all_users_permissions(self):
    """ Queries the UserAppServer and returns a list of all the users and the
      permissions they have in the system.

    Returns:
      A list of dicts, where each dict contains the e-mail address and
      authorizations that this user is granted in this AppScale deployment.
    """
    ret_list = []
    try:
      all_users_list = self.list_all_users()
      perm_items = self.get_all_permission_items()
      for user in all_users_list:
        usr_cap = {'email': user}
        caps_list = self.get_user_capabilities(user)
        for perm in perm_items:
          if perm in caps_list:
            usr_cap[perm] = True
          else:
            usr_cap[perm] = False
        ret_list.append(usr_cap)
    except Exception as err:
      logging.exception(err)
    return ret_list

  def get_all_permission_items(self):
    """ Returns a list of the capabilities that users can be granted.

    Returns:
      A list of strs, where each str is the name of a capability.
    """
    return ['upload_app']

  def add_user_permissions(self, email, perm):
    """ Grants the named capability to the specified user.

    Args:
      email: A str containing the e-mail address of the user who we wish to add
        a capability for.
      perm: A str containing the name of the capability to grant to the user.
    Returns:
      True if the permission was given to the user, and False otherwise.
    """
    try:
      caps_list = self.get_user_capabilities(email)
      uas = self.get_uaserver()
      new_caps = caps_list
      if perm not in new_caps:
        new_caps.append(perm)
      else:
        return True

      ret = uas.set_capabilities(email,
                                 self.USER_CAPABILITIES_DELIMITER.join(
                                   new_caps), GLOBAL_SECRET_KEY)
      if ret == 'true':
        self.cache['user_caps'][email] = new_caps
        return True
      else:
        logging.error("set_capabilities returned: {0}".format(ret))
        return False
    except Exception as err:
      logging.exception(err)
      return False

  def remove_user_permissions(self, email, perm):
    """ Revokes a capability from the specified user.

    Args:
      email: A str containing the e-mail address of the user who we wish to
        remove a permission from.
      perm: A str containing the name of the permission to remove from the user.
    Returns:
      True if the permission was removed from the user, and False otherwise.
    """
    try:
      caps_list = self.get_user_capabilities(email)
      uas = self.get_uaserver()
      if perm in caps_list:
        caps_list.remove(perm)
      else:
        return True

      ret = uas.set_capabilities(email,
                                 self.USER_CAPABILITIES_DELIMITER.join(
                                   caps_list), GLOBAL_SECRET_KEY)
      if ret == 'true':
        self.cache['user_caps'][email] = caps_list
        return True
      else:
        logging.error("remove_user_permissions returned: {0}".format(ret))
        return False
    except Exception as err:
      logging.exception(err)
      return False

  def gather_logs(self):
    """ Tells the AppController on this node to collect all log files we've
    accumulated so far in this AppScale deployment.

    Returns:
      A tuple containing two items. The first item is a bool that indicates if
        we were able to tell the AppController to gather the logs successfully,
        and the second item is a str that refers to the unique id that the logs'
        status can be queried at via REST, and can be used to construct a URL to
        download the logs once they are ready to download.
    """
    try:
      acc = self.get_appcontroller_client()
      uuid = acc.gather_logs()
      return True, uuid
    except Exception as err:
      logging.exception(err)
      return False, ""

  def run_groomer(self):
    """ Tells the AppController on this node to contact the machine running the
    Datastore on it, and instruct it to generate Kind statistics, for later
    viewing in the AppDashboard.

    Returns:
      'OK' if the request was successful, and in case of failures, the reason
      why the failure occurred.
    """
    try:
      acc = self.get_appcontroller_client()
      return acc.run_groomer()
    except Exception as err:
      logging.exception(err)
      return str(err)

  def change_password(self, email, password):
    """ Instructs the UserAppServer to set the given user's password to the
    given value.

    Args:
      email: A string indicating the email address of the user whose password
        should be reset.
      password: A string containing the cleartext password that should be set
        for the given user.
    Returns:
      A tuple containing a boolean and string. The boolean indicates whether the
      password reset was successful and the string indicates the reason why in
      the case of failure.
    """
    hashed_password = hashlib.sha1(email + password).hexdigest()

    try:
      user_app_server = self.get_uaserver()
      ret = user_app_server.change_password(email, hashed_password,
                                            GLOBAL_SECRET_KEY)
      if ret == "true":
        return True, "The user password was successfully changed."
      else:
        return False, ret
    except Exception as err:
      logging.exception(err)
      return False, "There was an error changing the user password."
