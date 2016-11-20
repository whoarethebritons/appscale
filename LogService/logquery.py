#!/usr/bin/python
import argparse
import socket
import struct
import time
from urlparse import urlparse

import capnp
import logging_capnp

from io import BytesIO

_I_SIZE = struct.calcsize('I')

def get_connection(args):
  url = urlparse(args.con)
  if url.scheme not in ('tcp', 'unix'):
    raise ValueError("Unsupported connection: %s" % args.con)
  if url.scheme == 'unix':
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(url.path)
  else:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(url.netloc.split(':', 2))
  sock.send('a%s%s' % (struct.pack('I', len(args.app_id)), args.app_id))
  return sock

def get_query(args, offset=None):
  query = logging_capnp.Query.new_message()
  if args.start:
    query.startTime = int(args.start)
  if args.end:
    query.endTime = int(args.end)
  query.versionIds = args.versions or []
  query.count = 1000 if args.count >= 1000 else args.count
  if offset:
    query.offset = offset
  return query.to_bytes()

def main(args):
  start = time.time()
  record_count = 0
  offset = None
  sock = get_connection(args)
  try:
    fh = sock.makefile()
    try:
      # send query
      while True:
        buf = get_query(args, offset)
        fh.write('q%s%s' % (struct.pack('I', len(buf)), buf))
        fh.flush()
        # receive results
        result_count, = struct.unpack('I', fh.read(_I_SIZE))
        if result_count == 0:
          break
        for _ in xrange(result_count):
          buflen, = struct.unpack('I', fh.read(_I_SIZE))
          record = logging_capnp.RequestLog.from_bytes(fh.read(buflen))
          time_seconds = (record.endTime or record.startTime) / 10**6
          date_string = time.strftime('%d/%b/%Y:%H:%M:%S %z',
                                      time.localtime(time_seconds))
          print '%s - %s [%s] "%s %s %s" %d %d - "%s"' % (
                record.ip, record.nickname, date_string, record.method, record.resource, 
                record.httpVersion, record.status or 0, record.responseSize or 0, record.userAgent)
          record_count += 1
          if record_count == args.count:
            break
        offset = record.offset
        if record_count == args.count:
          break
    finally:
      fh.close()
  finally:
    sock.close()
  print "Returned %s records in %s seconds" % (record_count, time.time() - start)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Query AppScale logserver.')
  parser.add_argument('--con', type=str, nargs='?', default='unix:///tmp/.appscale_logserver', help='Connection eg tcp://10.10.10.10:1010. (Default local log server)')
  parser.add_argument('--start', type=int, nargs='?', help='start epoch timestamp')
  parser.add_argument('--end', type=int, nargs='?', help='end epoch timestamp')
  parser.add_argument('--ids', type=str, nargs='+', help='requestIds')
  parser.add_argument('--count', type=int, nargs='?', help='count', default=10)
  parser.add_argument('app_id', type=str, help='app_id')
  parser.add_argument('versions', type=str, nargs='+', help='app versions')
  args = parser.parse_args()
  #import pdb; pdb.set_trace()
  main(args)

