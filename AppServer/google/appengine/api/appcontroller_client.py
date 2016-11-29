#!/usr/bin/env python
""" Interface to the AppController. """

# General-purpose Python library imports
import json
import socket
import ssl
import sys


# Third-party imports
import SOAPpy


# AppScale-specific imports
class AppControllerException(Exception):
  """ AppController exception. """


class AppControllerClient():
  """AppControllerClient provides callers with an interface to AppScale's
  AppController daemon.

  The AppController is a dispatching service that is responsible for starting
  API services on each node in an AppScale deployment. Callers may talk to
  the AppController to get information about the number of nodes in the
  deployment as well as what services each node runs.
  """

  # The port that the AppController runs on by default.
  PORT = 17443

  # Maximum number of times we try a call to the AppController.
  MAX_RETRIES = 3

  # The number of seconds we should wait for when waiting for the UserAppServer
  # to start up.
  WAIT_TIME = 10

  # The message that an AppController can return if callers do not authenticate
  # themselves correctly.
  BAD_SECRET_MESSAGE = 'false: bad secret'

  # An AppController response that indicates Nginx and HAProxy are not set up.
  NOT_READY = 'false: not ready yet'

  def __init__(self, host, secret):
    """Creates a new AppControllerClient.

    Args:
      host: The location where an AppController can be found.
      secret: A str containing the secret key, used to authenticate this client
        when talking to remote AppControllers.
    """
    self.host = host
    self.server = SOAPpy.SOAPProxy('https://{0}:{1}'.format(host, self.PORT))
    self.secret = secret


  def call(self, retries, function, *args):
    """Runs the given function, retrying it if a transient error is seen.

    Args:
      retries: The number of times to retry.
      function: The function that should be executed.
      *args: The arguments that will be passed to function.
    Returns:
      The return value of function(*args).
    Raises:
      AppControllerException: If the AppController we're trying to connect to is
        not running at the given IP address, or if it rejects the SOAP request.
    """
    if retries <= 0:
      raise AppControllerException(
        "Ran out of retries calling the AppController. ")

    try:
      result = function(*args)

      if result == self.BAD_SECRET_MESSAGE:
        raise AppControllerException("Could not authenticate successfully" + \
          " to the AppController. You may need to change the keyname in use.")
      else:
        return result
    except (ssl.SSLError, socket.error):
      sys.stderr.write("Saw SSL exception when communicating with the " \
        "AppController, retrying momentarily.")
      return self.call(retries - 1, function, *args)


  def set_parameters(self, layout, options, app=None):
    """Passes the given parameters to an AppController, allowing it to start
    configuring API services in this AppScale deployment.

    Args:
      layout: A list that contains the first node's IP address.
      options: A list that contains API service-level configuration info,
        as well as a mapping of IPs to the API services they should host
        (excluding the first node).
    Raises:
      AppControllerException: If the remote AppController indicates that there
        was a problem with the parameters passed to it.
    """
    if app is None:
      app = 'none'

    result = self.call(self.MAX_RETRIES, self.server.set_parameters, layout,
      options, [app], self.secret)
    if result.startswith('Error'):
      raise AppControllerException(result)


  def get_all_public_ips(self):
    """Queries the AppController for a list of all the machines running in this
    AppScale deployment, and returns their public IP addresses.

    Returns:
      A list of the public IP addresses of each machine in this AppScale
      deployment.
    """
    return json.loads(self.call(self.MAX_RETRIES,
      self.server.get_all_public_ips, self.secret))


  def get_role_info(self):
    """Queries the AppController to determine what each node in the deployment
    is doing and how it can be externally or internally reached.

    Returns:
      A dict that contains the public IP address, private IP address, and a list
      of the API services that each node runs in this AppScale deployment.
    """
    return json.loads(self.call(self.MAX_RETRIES, self.server.get_role_info,
      self.secret))


  def get_status(self):
    """Queries the AppController to learn information about the machine it runs
    on.

    This includes information about the CPU, memory, and disk of that machine,
    as well as what machine that AppController connects to for database access
    (via the UserAppServer).

    Returns:
      A str containing information about the CPU, memory, and disk usage of that
      machine, as well as where the UserAppServer is located.
    """
    return self.call(self.MAX_RETRIES, self.server.status, self.secret)


  def get_api_status(self):
    """Queries the AppController to see what the status of Google App Engine
    APIs are in this AppScale deployment, reported to it by the API Checker.

    APIs can be either 'running', 'failed', or 'unknown' (which typically
    occurs when AppScale is first starting up).

    Returns:
      A dict that maps each API name (a str) to its status (also a str).
    """
    return json.loads(self.call(self.MAX_RETRIES, self.server.get_api_status,
      self.secret))


  def get_database_information(self):
    """Queries the AppController to see what database is being used to implement
    support for the Google App Engine Datastore API, and how many replicas are
    present for each piece of data.

    Returns:
      A dict that indicates both the name of the database in use (with the key
      'table', for historical reasons) and the replication factor (with the
      key 'replication').
    """
    return json.loads(self.call(self.MAX_RETRIES,
      self.server.get_database_information, self.secret))

  def relocate_app(self, appid, http_port, https_port):
    """Asks the AppController to start serving traffic for the named application
    on the given ports, instead of the ports that it was previously serving at.

    Args:
      appid: A str that names the already deployed application that we want to
        move to a different port.
      http_port: An int between 80 and 90, or between 1024 and 65535, that names
        the port that unencrypted traffic should be served from for this app.
      https_port: An int between 443 and 453, or between 1024 and 65535, that
        names the port that encrypted traffic should be served from for this
        app.
    Returns:
      A str that indicates if the operation was successful, and in unsuccessful
      cases, the reason why the operation failed.
    """
    res = (self.call(self.MAX_RETRIES, self.server.relocate_app,
                     appid, http_port, https_port, self.secret))
    return res

  def upload_app(self, filename, file_suffix, email):
    """Tells the AppController to use the AppScale Tools to upload the Google
    App Engine application at the specified location.

    Args:
      filename: A str that points to a compressed file on the local filesystem
        containing the user's Google App Engine application.
      file_suffix: A str that names the suffix this file should have.
      email: A str containing an e-mail address that should be registered as the
        administrator of this application.
    Returns:
      A str that indicates either that the app was successfully uploaded, or the
      reason why the application upload failed.
    """
    return json.loads(self.call(self.MAX_RETRIES, self.server.upload_app,
      filename, file_suffix, email, self.secret))


  def get_app_upload_status(self, reservation_id):
    """Queries the AppController to see if the App Engine app corresponding to
    the given reservation ID has been successfully uploaded.

    Args:
      reservation_id: A str that corresponds to the App Engine app being
        uploaded, likely given to the caller from the initial upload SOAP call.
    Returns:
      A str with the status of the application being uploaded.
    """
    return self.call(self.MAX_RETRIES, self.server.get_app_upload_status,
      reservation_id, self.secret)


  def get_stats(self):
    """Queries the AppController to get server-level statistics and a list of
    App Engine apps running in this cloud deployment across all machines.

    Returns:
      A list of dicts, where each dict contains server-level statistics (e.g.,
        CPU, memory, disk usage) about one machine.
    """
    return json.loads(self.call(self.MAX_RETRIES, self.server.get_stats_json,
      self.secret))


  def is_initialized(self):
    """Queries the AppController to see if it has started up all of the API
    services it is responsible for on its machine.

    Returns:
      A bool that indicates if all API services have finished starting up on
      this machine.
    """
    return self.call(self.MAX_RETRIES, self.server.is_done_initializing,
      self.secret)


  def start_roles_on_nodes(self, roles_to_nodes):
    """Dynamically adds the given machines to an AppScale deployment, with the
    specified roles.

    Args:
      A JSON-dumped dict that maps roles to IP addresses.
    Returns:
      The result of executing the SOAP call on the remote AppController.
    """
    return self.call(self.MAX_RETRIES, self.server.start_roles_on_nodes,
      roles_to_nodes, self.secret)


  def stop_app(self, app_id):
    """Tells the AppController to no longer host the named application.

    Args:
      app_id: A str that indicates which application should be stopped.
    Returns:
      The result of telling the AppController to no longer host the app.
    """
    return self.call(self.MAX_RETRIES, self.server.stop_app, app_id,
      self.secret)


  def is_app_running(self, app_id):
    """Queries the AppController to see if the named application is running.

    Args:
      app_id: A str that indicates which application we should be checking
        for.
    Returns:
      True if the application is running, False otherwise.
    """
    return self.call(self.MAX_RETRIES, self.server.is_app_running, app_id,
      self.secret)


  def done_uploading(self, app_id, remote_app_location):
    """Tells the AppController that an application has been uploaded to its
    machine, and where to find it.

    Args:
      app_id: A str that indicates which application we have copied over.
      remote_app_location: A str that indicates the location on the remote
        machine where the App Engine application can be found.
    """
    return self.call(self.MAX_RETRIES, self.server.done_uploading, app_id,
      remote_app_location, self.secret)


  def update(self, apps_to_run):
    """Tells the AppController which applications to run, which we assume have
    already been uploaded to that machine.

    Args:
      apps_to_run: A list of apps to start running on nodes running the App
        Engine service.
    """
    return self.call(self.MAX_RETRIES, self.server.update, apps_to_run,
      self.secret)


  def gather_logs(self):
    """ Tells the AppController to copy logs from all machines to a tar.gz file
    stored in the AppDashboard's static file directory, so that users can
    download it.
    """
    return self.call(self.MAX_RETRIES, self.server.gather_logs, self.secret)


  def run_groomer(self):
    """ Tells the AppController to clean up entities in the Datastore that have
    been soft deleted, and to generate statistics about the entities still in
    the Datastore (which can be viewed in the AppDashboard).
    """
    return self.call(self.MAX_RETRIES, self.server.run_groomer, self.secret)


  def add_routing_for_appserver(self, app_id, appserver_ip, port):
    """ Tells the AppController to begin routing traffic to an AppServer.

    Args:
      app_id: A string that contains the application ID.
      appserver_ip: A string that contains the IP address of the instance
        running the AppServer.
      port: A string that contains the port that the AppServer listens on.
    """
    return self.call(self.MAX_RETRIES, self.server.add_routing_for_appserver,
      app_id, appserver_ip, port, self.secret)


  def add_routing_for_blob_server(self):
    """ Tells the AppController to begin routing traffic to the
        BlobServer(s).
    """
    return self.call(self.MAX_RETRIES, self.server.add_routing_for_blob_server,
      self.secret)


  def remove_appserver_from_haproxy(self, app_id, appserver_ip, port):
    """ Tells the AppController to stop routing traffic to an AppServer.

    Args:
      app_id: A string that contains the application ID.
      appserver_ip: A string that contains the IP address of the instance
        running the AppServer.
      port: A string that contains the port that the AppServer listens on.
    """
    return self.call(self.MAX_RETRIES,
      self.server.remove_appserver_from_haproxy, app_id, appserver_ip, port,
      self.secret)


  def deployment_id_exists(self):
    """ Asks the AppController if the deployment ID is stored in ZooKeeper.

    Returns:
      A boolean indicating whether the deployment ID is stored or not.
    """
    return self.call(self.MAX_RETRIES, self.server.deployment_id_exists,
      self.secret)


  def get_deployment_id(self):
    """ Retrieves the deployment ID from ZooKeeper.

    Returns:
      A string containing the deployment ID.
    """
    return self.call(self.MAX_RETRIES, self.server.get_deployment_id,
      self.secret)


  def set_read_only(self, read_only):
    """ Enables or disables datastore writes for the deployment.

    Args:
      read_only: A string that indicates whether or to turn read-only mode on
        or off.
    """
    return self.call(self.MAX_RETRIES, self.server.set_read_only, read_only,
      self.secret)
