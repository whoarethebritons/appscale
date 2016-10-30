#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Stub implementation for Log Service that uses sqlite."""

import time
import os
import socket
from collections import defaultdict

import capnp

from google.appengine.api import apiproxy_stub
from google.appengine.api.logservice import log_service_pb
from google.appengine.runtime import apiproxy_errors

import logging_capnp

class LogServiceStub(apiproxy_stub.APIProxyStub):
  """Python stub for Log Service service."""

  _LOGSERVER_PATH = '/tmp/.appscale_logserver'

  THREADSAFE = True

  _ACCEPTS_REQUEST_ID = True


  _DEFAULT_READ_COUNT = 20


  def __init__(self, persist=False, logs_path=None, request_data=None):
    """Initializer.

    Args:
      persist: For backwards compatability. Has no effect.
      logs_path: A str containing the filename to use for logs storage. Defaults
        to in-memory if unset.
      request_data: A apiproxy_stub.RequestData instance used to look up state
        associated with the request that generated an API call.
    """

    super(LogServiceStub, self).__init__('logservice',
                                         request_data=request_data)
    self._pending_requests = defaultdict(logging_capnp.RequestLog.new_message)
    self._pending_requests_applogs = dict()
    self._log_server = dict()

  def _get_log_server(self, app_id, blocking):
    key = (blocking, app_id)
    if key in self._log_server:
      return self._log_server[key]
    if os.path.exists(_LOGSERVER_PATH):
      client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
      try:
        client.connect(_LOGSERVER_PATH)
        client.setblocking(blocking)
        client.send('a%s%s' % (struct.pack('I', len(app_id)), app_id)) 
        self._log_server[key] = client
        return client
      except socket.error, e:
        return None
    return None

  def _cleanup_logserver_connection(app_id, blocking):
    key = (blocking, app_id)
    log_server = self._log_server.get(key)
    if log_server:
      try:
        log_server.close()
      except socker.error:
        pass
      del self._log_server[key]

  def _send_to_logserver(self, api_id, packet):
    log_server = self._get_log_server(app_id, False)
    if log_server:
      try:
        log_server.send(packet)
      except socket.error, e:
        self._cleanup_logserver_connection(app_id, False)
        self._send_to_logserver(app_id, type_, packet)

  @staticmethod
  def _get_time_usec():
    return int(time.time() * 1e6)

  @apiproxy_stub.Synchronized
  def start_request(self, request_id, user_request_id, ip, app_id, version_id,
                    nickname, user_agent, host, method, resource, http_version,
                    start_time=None):
    """Starts logging for a request.

    Each start_request call must be followed by a corresponding end_request call
    to cleanup resources allocated in start_request.

    Args:
      request_id: A unique string identifying the request associated with the
        API call.
      user_request_id: A user-visible unique string for retrieving the request
        log at a later time.
      ip: The user's IP address.
      app_id: A string representing the application ID that this request
        corresponds to.
      version_id: A string representing the version ID that this request
        corresponds to.
      nickname: A string representing the user that has made this request (that
        is, the user's nickname, e.g., 'foobar' for a user logged in as
        'foobar@gmail.com').
      user_agent: A string representing the agent used to make this request.
      host: A string representing the host that received this request.
      method: A string containing the HTTP method of this request.
      resource: A string containing the path and query string of this request.
      http_version: A string containing the HTTP version of this request.
      start_time: An int containing the start time in micro-seconds. If unset,
        the current time is used.
    """
    major_version_id = version_id.split('.', 1)[0]
    if start_time is None:
      start_time = self._get_time_usec()

    rl = self._pending_requests[request_id]
    rl.appId = app_id
    rl.versionId = version_id
    rl.requestId = request_id
    rl.ip = ip
    rl.nickname = nickname
    rl.startTime = start_time
    rl.method = method
    rl.resource = resource
    rl.httpVersion = htpp_version
    rl.userAgent = user_agent
    rl.host = host
    self._pending_requests_applogs[request_id] = rl.init_resizable_list('appLogs')

  @apiproxy_stub.Synchronized
  def end_request(self, request_id, status, response_size, end_time=None):
    """Ends logging for a request.

    Args:
      request_id: A unique string identifying the request associated with the
        API call.
      status: An int containing the HTTP status code for this request.
      response_size: An int containing the content length of the response.
      end_time: An int containing the end time in micro-seconds. If unset, the
        current time is used.
    """
    if end_time is None:
      end_time = self._get_time_usec()
    rl = self._pending_requests.get(request_id, None)
    if rl is None:
        raise ValueError('Expected LogRequest object in cache!')  # Crash as this should not happen.
    rl.status = status
    rl.responseSize = response_size
    rl.endTime = end_time
    self._pending_requests_applogs[request_id].finish()
    buf = rl.to_bytes()
    packet = 'l%s%s' % (struct.pack('I', len(buf)), buf)
    self._send_to_logserver(rl.appId, packet)
    del self._pending_requests_applogs[request_id]
    del self._pending_requests[request_id]

  def _Dynamic_Flush(self, request, unused_response, request_id):
    """Writes application-level log messages for a request."""
    rl = self._pending_requests.get('request_id', None)
    if rl is None:
        raise ValueError('Expected LogRequest object in cache!')  # Crash as this should not happen.
    time, level, message = request
    al = self._pending_requests_applogs[request_id].add()
    al.time = time
    al.level = level
    al.message = message

  @apiproxy_stub.Synchronized
  def _Dynamic_Read(self, request, response, request_id):
    if (request.module_version_size() < 1 and
        request.version_id_size() < 1 and
        request.request_id_size() < 1):
      raise apiproxy_errors.ApplicationError(
          log_service_pb.LogServiceError.INVALID_REQUEST)

    if request.module_version_size() > 0 and request.version_id_size() > 0:
      raise apiproxy_errors.ApplicationError(
          log_service_pb.LogServiceError.INVALID_REQUEST)

    if (request.request_id_size() and
        (request.has_start_time() or request.has_end_time() or
         request.has_offset())):
      raise apiproxy_errors.ApplicationError(
          log_service_pb.LogServiceError.INVALID_REQUEST)

    if request.request_id_size():
      for request_id in request.request_id_list():
        log_row = self._conn.execute(
            'SELECT * FROM RequestLogs WHERE user_request_id = ?',
            (request_id,)).fetchone()
        if log_row:
          log = response.add_log()
          self._fill_request_log(log_row, log, request.include_app_logs())
      return

    if request.has_count():
      count = request.count()
    else:
      count = self._DEFAULT_READ_COUNT
    filters = self._extract_read_filters(request)
    filter_string = ' WHERE %s' % ' and '.join(f[0] for f in filters)

    if request.has_minimum_log_level():
      query = ('SELECT * FROM RequestLogs INNER JOIN AppLogs ON '
               'RequestLogs.id = AppLogs.request_id%s GROUP BY '
               'RequestLogs.id ORDER BY id DESC')
    else:
      query = 'SELECT * FROM RequestLogs%s ORDER BY id DESC'
    logs = self._conn.execute(query % filter_string,
                              tuple(f[1] for f in filters)).fetchmany(count + 1)
    for log_row in logs[:count]:
      log = response.add_log()
      self._fill_request_log(log_row, log, request.include_app_logs())
    if len(logs) > count:
      response.mutable_offset().set_request_id(str(logs[-2]['id']))

  def _fill_request_log(self, log_row, log, include_app_logs):
    log.set_request_id(str(log_row['user_request_id']))
    log.set_app_id(log_row['app_id'])
    log.set_version_id(log_row['version_id'])
    log.set_ip(log_row['ip'])
    log.set_nickname(log_row['nickname'])
    log.set_start_time(log_row['start_time'])
    log.set_host(log_row['host'])
    log.set_end_time(log_row['end_time'])
    log.set_method(log_row['method'])
    log.set_resource(log_row['resource'])
    log.set_status(log_row['status'])
    log.set_response_size(log_row['response_size'])
    log.set_http_version(log_row['http_version'])
    log.set_user_agent(log_row['user_agent'])
    log.set_url_map_entry(log_row['url_map_entry'])
    log.set_latency(log_row['latency'])
    log.set_mcycles(log_row['mcycles'])
    log.set_finished(log_row['finished'])
    log.mutable_offset().set_request_id(str(log_row['id']))
    time_seconds = (log_row['end_time'] or log_row['start_time']) / 10**6
    date_string = time.strftime('%d/%b/%Y:%H:%M:%S %z',
                                time.localtime(time_seconds))
    log.set_combined('%s - %s [%s] "%s %s %s" %d %d - "%s"' %
                     (log_row['ip'], log_row['nickname'], date_string,
                      log_row['method'], log_row['resource'],
                      log_row['http_version'], log_row['status'] or 0,
                      log_row['response_size'] or 0, log_row['user_agent']))
    if include_app_logs:
      log_messages = self._conn.execute(
          'SELECT timestamp, level, message FROM AppLogs '
          'WHERE request_id = ?',
          (log_row['id'],)).fetchall()
      for message in log_messages:
        line = log.add_line()
        line.set_time(message['timestamp'])
        line.set_level(message['level'])
        line.set_log_message(message['message'])

  @staticmethod
  def _extract_read_filters(request):


    if request.module_version(0).has_module_id():
      module_version = ':'.join([request.module_version(0).module_id(),
                                 request.module_version(0).version_id()])
    else:
      module_version = request.module_version(0).version_id()
    filters = [('version_id = ?', module_version)]
    if request.has_start_time():
      filters.append(('start_time >= ?', request.start_time()))
    if request.has_end_time():
      filters.append(('end_time < ?', request.end_time()))
    if request.has_offset():
      filters.append(('RequestLogs.id < ?', int(request.offset().request_id())))
    if not request.include_incomplete():
      filters.append(('finished = ?', 1))
    if request.has_minimum_log_level():
      filters.append(('AppLogs.level >= ?', request.minimum_log_level()))
    return filters

  def _Dynamic_SetStatus(self, unused_request, unused_response,
                         unused_request_id):
    raise NotImplementedError

  def _Dynamic_Usage(self, unused_request, unused_response, unused_request_id):
    raise apiproxy_errors.CapabilityDisabledError('Usage not allowed in tests.')
