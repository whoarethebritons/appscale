import struct
import os
import re
from collections import defaultdict
from cStringIO import StringIO

import capnp
import logging_capnp

from twisted.internet import reactor, protocol
from twisted.python import log

MAX_LOG_FILE_SIZE = 1024 * 1024 * 1024

class AppRegistry(object):

    def __init__(self):
        self.logfiles = []
        self.writeHandle = None
        self.indexRequestIds = dict()
        self.indexRequestTime = defaultdict(StringIO)

class Protocol(protocol.Protocol):

    def __init__(self):
        self.buf = ''
        self.app_id = None
        self.app_registry = None

    def dataReceived(self, data):
        self.buf += data
        self.processActions()

    def processActions(self):
        buffer_size = len(self.buf)
        if buffer_size < 5:
            return
        action = self.buf[0]
        query_length, = struct.unpack('I', self.buf[1:5])
        query_end = query_length + 5;
        if buffer_size < query_end:
            return
        query = self.buf[5:query_end]
        processor = self.ACTIONS.get(action)
        if processor:
            processor(self, query)
        else:
            log.error("Received unknown action %s", action)
            self.transport.loseConnection()
        self.buf = self.buf[query_end:]

    def processSetAppId(self, query):
        # Set our app_id
        self.app_id = query
        # Rebuild in memory indexes
        self.app_registry = self.factory.apps[self.app_id]
        self.app_registry.log_files = [f for f in os.listdir(self.factory.path) if re.match('^logservice_%s\\.\\d+\\.log$' % self.app_id, f)]
        self.app_registry.log_files.sort(key=lambda x: int(re.match('^logservice_%s\\.(\\d+)\\.log$' % self.app_id, x).groups()[0]))
        log_file_index = 0
        for log_file in self.app_registry.log_files:
            log_file_index += 1
            with open(os.path.join(self.factory.path, logfile), 'rb') as fh:
                position = 0
                while True:
                   buf = fh.read(4)
                   if not buf:
                       break
                   record_length, = struct.unpack('I', buf)
                   buf = fh.read(record_length)
                   record = logging_capnp.RequestLog.from_bytes(buf)
                   self.index(log_file_index, position, record)
                   position += 4 + len(buf)

    def index(self, log_file_index, position, record):
        self.app_registry.indexRequestIds[record.requestId] = struct.pack('HI', log_file_index, position)
        self.app_registry.indexRequestTime[int(record.startTime)].write(struct.pack('dHI', record.startTime, log_file_index, position))


    def processActionLog(self, query):
        if not self.app_id:
            log.error("Don't know what to do, because the client did not set the app_id yet! Closing the connection ...")
            self.transport.loseConnection()
            return
        if not self.app_registry.writeHandle or self.app_registry.writeHandle.tell() > MAX_LOG_FILE_SIZE:
            if not self.app_registry.logfiles:
                self.app_registry.logfiles.append(os.path.join(self.factory.path, 'logservice_%s.1.log' % self.app_id))
            fh = open(self.app_registry.logfiles[-1], 'ab')
            if fh.tell() > MAX_LOG_FILE_SIZE:
                fh.close()
                next_logfile_id = int(re.match('/logservice_%s\\.(\\d+)\\.log$' % self.app_id, self.app_registry.logfiles[-1]).groups()[0]) + 1
                self.app_registry.logfiles.append(os.path.join(self.factory.path, 'logservice_%s.%s.log' % (self.app_id, next_log_file_id)))
                fh = open(self.app_registry.logfiles[-1], 'ab')
            if self.app_registry.writeHandle:
                self.app_registry.writeHandle.close()
            self.app_registry.writeHandle = fh
        position = self.app_registry.writeHandle.tell()
        log_file_index = len(self.app_registry.logfiles) - 1
        self.app_registry.writeHandle.write(struct.pack('I', len(query)))
        self.app_registry.writeHandle.write(query)
        self.app_registry.writeHandle.flush()
        record = logging_capnp.RequestLog.from_bytes(query)
        self.index(log_file_index, position, record)
    
    ACTIONS=dict(l=processActionLog, a=processSetAppId)


class LogServerFactory(protocol.Factory):
    protocol = Protocol

    def __init__(self, path, size):
        self.path = path
        self.size = size
        self.apps = defaultdict(AppRegistry)
