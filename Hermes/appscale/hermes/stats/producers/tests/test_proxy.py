import os
from os import path
import unittest

from mock import patch, MagicMock

from appscale.hermes.stats.constants import MISSED
from appscale.hermes.stats.producers import proxy_stats

CUR_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(CUR_DIR, 'test-data')


class TestCurrentProxiesStats(unittest.TestCase):

  def setUp(self):
    self.stats_file = None

  def tearDown(self):
    if self.stats_file:
      self.stats_file.close()

  @patch.object(proxy_stats.socket, 'socket')
  def test_haproxy_stats_v1_5(self, mock_socket):
    # Mocking haproxy stats socket with csv file
    self.stats_file = open(path.join(TEST_DATA_DIR, 'haproxy-stats-v1.5.csv'))
    fake_socket = MagicMock(recv=self.stats_file.read)
    mock_socket.return_value = fake_socket

    # Running method under test
    stats_snapshot = proxy_stats.ProxiesStatsSource().get_current()

    # Verifying outcomes
    self.assertIsInstance(stats_snapshot.utc_timestamp, float)
    proxies_stats = stats_snapshot.proxies_stats
    self.assertEqual(len(proxies_stats), 5)
    proxies_stats_dict = {
      proxy_stats.name: proxy_stats for proxy_stats in proxies_stats
    }
    self.assertEqual(set(proxies_stats_dict), {
      'TaskQueue', 'UserAppServer', 'appscale-datastore_server',
      'as_blob_server', 'gae_appscaledashboard'
    })

    # There are 5 proxies, let's choose one for deeper verification
    dashboard = proxies_stats_dict['gae_appscaledashboard']
    self.assertEqual(dashboard.name, 'gae_appscaledashboard')
    self.assertEqual(dashboard.unified_service_name, 'application')
    self.assertEqual(dashboard.application_id, 'appscaledashboard')

    # Frontend stats shouldn't have Nones
    frontend = dashboard.frontend
    for field in proxy_stats.HAProxyFrontendStats.__slots__:
      self.assertIsNotNone(getattr(frontend, field))

    # Backend stats shouldn't have Nones
    backend = dashboard.backend
    for field in proxy_stats.HAProxyBackendStats.__slots__:
      self.assertIsNotNone(getattr(backend, field))

    # Backend stats can have Nones only in some fields
    servers = dashboard.servers
    self.assertIsInstance(servers, list)
    self.assertEqual(len(servers), 3)
    for server in servers:
      for field in proxy_stats.HAProxyServerStats.__slots__:
        if field in {'qlimit', 'throttle', 'tracked', 'check_code',
                     'last_chk', 'last_agt'}:
          continue
        self.assertIsNotNone(getattr(server, field))

    # We don't have listeners on stats
    self.assertEqual(dashboard.listeners, [])

  @patch.object(proxy_stats.socket, 'socket')
  @patch.object(proxy_stats.logging, 'warn')
  def test_haproxy_stats_v1_4(self, mock_logging_warn, mock_socket):
    # Mocking "echo 'show stat' | socat stdio unix-connect:{}" with csv file
    self.stats_file = open(path.join(TEST_DATA_DIR, 'haproxy-stats-v1.4.csv'))
    fake_socket = MagicMock(recv=self.stats_file.read)
    mock_socket.return_value = fake_socket

    # Running method under test
    stats_snapshot = proxy_stats.ProxiesStatsSource().get_current()

    # Verifying outcomes
    self.assertIsInstance(stats_snapshot.utc_timestamp, float)
    proxies_stats = stats_snapshot.proxies_stats
    mock_logging_warn.assert_called_once_with(
      "HAProxy stats fields ['rtime', 'ctime', 'comp_in', 'qtime', 'comp_byp', "
      "'lastsess', 'comp_rsp', 'last_chk', 'ttime', 'comp_out', 'last_agt'] "
      "are missed. Old version of HAProxy is probably used (v1.5+ is expected)"
    )
    self.assertEqual(len(proxies_stats), 5)
    proxies_stats_dict = {
      proxy_stats.name: proxy_stats for proxy_stats in proxies_stats
    }
    self.assertEqual(set(proxies_stats_dict), {
      'TaskQueue', 'UserAppServer', 'appscale-datastore_server',
      'as_blob_server', 'gae_appscaledashboard'
    })

    # There are 5 proxies, let's choose one for deeper verification
    dashboard = proxies_stats_dict['gae_appscaledashboard']
    self.assertEqual(dashboard.name, 'gae_appscaledashboard')
    self.assertEqual(dashboard.unified_service_name, 'application')
    self.assertEqual(dashboard.application_id, 'appscaledashboard')

    # Frontend stats shouldn't have Nones
    frontend = dashboard.frontend
    for field in proxy_stats.HAProxyFrontendStats.__slots__:
      self.assertIsNotNone(getattr(frontend, field))
    # New columns should be highlighted
    for new_in_v1_5 in ('comp_byp', 'comp_rsp', 'comp_out', 'comp_in'):
      self.assertIs(getattr(frontend, new_in_v1_5), MISSED)

    # Backend stats shouldn't have Nones
    backend = dashboard.backend
    for field in proxy_stats.HAProxyBackendStats.__slots__:
      self.assertIsNotNone(getattr(backend, field))
    # New columns should be highlighted
    for new_in_v1_5 in ('comp_byp', 'lastsess', 'comp_rsp', 'comp_out',
                        'comp_in', 'ttime', 'rtime', 'ctime', 'qtime'):
      self.assertIs(getattr(backend, new_in_v1_5), MISSED)

    # Backend stats can have Nones only in some fields
    servers = dashboard.servers
    self.assertIsInstance(servers, list)
    self.assertEqual(len(servers), 3)
    for server in servers:
      for field in proxy_stats.HAProxyServerStats.__slots__:
        if field in {'qlimit', 'throttle', 'tracked', 'check_code',
                     'last_chk', 'last_agt'}:
          continue
        self.assertIsNotNone(getattr(server, field))
      # New columns should be highlighted
      for new_in_v1_5 in ('lastsess', 'last_chk', 'ttime', 'last_agt',
                          'rtime', 'ctime', 'qtime'):
        self.assertIs(getattr(server, new_in_v1_5), MISSED)

    # We don't have listeners on stats
    self.assertEqual(dashboard.listeners, [])
