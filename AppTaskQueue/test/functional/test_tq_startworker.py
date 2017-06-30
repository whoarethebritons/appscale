#!/usr/bin/env python

import json
import os
import socket
import unittest
import urllib2

from appscale.common import file_io

FILE_LOC = "/var/apps/test_app/app/queue.yaml"
def create_test_yaml():
  file_loc = FILE_LOC
  config = \
"""
queue:
- name: default
  rate: 5/s
- name: foo
  rate: 10/m
"""
  try:
    os.mkdir("/var/apps/test_app")
    os.mkdir("/var/apps/test_app/app/")
  except OSError:
    pass
  file_io.write(file_loc, config)

# AppScale must already be running with RabbitMQ
class TestTaskQueueServer(unittest.TestCase):
  def test_slave(self):
    create_test_yaml()
    values = {'app_id':'test_app'}
    host = socket.gethostbyname(socket.gethostname())
    req = urllib2.Request('http://' + host + ':17446/startworker')
    req.add_header('Content-Type', 'application/json')
    response = urllib2.urlopen(req, json.dumps(values))
    print response.read()
    self.assertEquals(response.getcode(), 200)
             
if __name__ == "__main__":
  unittest.main()
