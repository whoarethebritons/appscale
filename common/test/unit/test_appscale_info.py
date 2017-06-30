import json
import unittest
from flexmock import flexmock

from appscale.common import appscale_info
from appscale.common import file_io


class TestAppScaleInfo(unittest.TestCase):
  def test_get_num_cpus(self):
    self.assertNotEqual(0, appscale_info.get_num_cpus())

  def test_stop(self):
    YAML_INFO="""--- 
:keyname: appscale
:replication: "1"
:table: cassandra
"""
    flexmock(file_io).should_receive('read').and_return(YAML_INFO)
    self.assertEqual('cassandra', appscale_info.get_db_info()[':table'])
    self.assertEqual( '1', appscale_info.get_db_info()[':replication'])
    self.assertEqual( 'appscale', appscale_info.get_db_info()[':keyname'])
    self.assertEqual(True, isinstance(appscale_info.get_db_info(), dict))

  def test_get_all_ips(self):
    flexmock(file_io).should_receive("read"). \
      and_return("192.168.0.1\n129.168.0.2\n184.48.65.89")
    self.assertEquals(["192.168.0.1", "129.168.0.2", "184.48.65.89"],
      appscale_info.get_all_ips())

  def test_get_taskqueue_nodes(self):
    flexmock(file_io).should_receive("mkdir").and_return(None)
    flexmock(file_io).should_receive("read").\
      and_return("192.168.0.1\n129.168.0.2\n184.48.65.89")
    self.assertEquals(["192.168.0.1","129.168.0.2","184.48.65.89"],
      appscale_info.get_taskqueue_nodes())

    flexmock(file_io).should_receive("read").\
      and_return("192.168.0.1\n129.168.0.2\n184.48.65.89\n")
    self.assertEquals(["192.168.0.1","129.168.0.2","184.48.65.89"],
      appscale_info.get_taskqueue_nodes())

    flexmock(file_io).should_receive("read").and_return("")
    self.assertEquals(appscale_info.get_taskqueue_nodes(), [])

  def test_get_db_proxy(self):
    flexmock(file_io).should_receive("read").\
      and_return("192.168.0.1\n129.168.0.2\n184.48.65.89")
    self.assertEquals("192.168.0.1", appscale_info.get_db_proxy())

  def test_get_tq_proxy(self):
    flexmock(file_io).should_receive("read").\
      and_return("192.168.0.1\n129.168.0.2\n184.48.65.89")
    self.assertEquals("192.168.0.1", appscale_info.get_db_proxy())

  def test_get_zk_node_ips(self):
    flexmock(file_io).should_receive("read").\
      and_return({"locations":["ip1", "ip2"],"last_updated_at":0})
    flexmock(json).should_receive("loads").\
      and_return({"locations":[u'ip1', u'ip2'],"last_updated_at":0})
    self.assertEquals(appscale_info.get_zk_node_ips(), [u'ip1', u'ip2'])

    flexmock(file_io).should_receive("read").and_raise(IOError)
    self.assertEquals(appscale_info.get_zk_node_ips(), [])

  def test_get_search_location(self):
    flexmock(file_io).should_receive("read").and_return("private_ip:port")
    self.assertEquals(appscale_info.get_search_location(), "private_ip:port")

    flexmock(file_io).should_receive("read").and_raise(IOError)
    self.assertEquals(appscale_info.get_search_location(), "")

if __name__ == "__main__":
  unittest.main()
