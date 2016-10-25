import struct
import os
from collections import defaultdict
from cStringIO import StringIO

import capnp
import logging_capnp

from twisted.internet import reactor, protocol
from twisted.logger import Logger
log = Logger()

class Protocol(protocol.Protocol):

    def __init__(self):
        self.buf = ''
        self.app_id = None
        self.logfiles = []
        self.indexRequestIds = dict()
        self.indexRequestTime = defaultdict(StringIO)

    def dataReceived(self, data):
        self.buf += data
        self.processActions()

    def processActions(self):
        buffer_size = len(self.buf)
        if buffer_size < 5:
            return
        action = self.buf[0]
        query_length = struct.unpack('I', self.buf[1:5])
        query_end = query_length + 5;
        if buffer_size < query_end:
            return
        query = self.buf[5:query_end]
        processor = self.ACTIONS.get(action)
        if processor:
            pocessor(self, query)
        else:
            log.error("Received unknown action %s", action)
            self.transport.loseConnection()
        self.buf = self.buf[query_end:]

    def processSetAppId(self, query):
        # Set our app_id
        self.app_id = query
        # Rebuild in memory indexes
        self.log_files = [f for f in os.listdir(self.factory.path) if re.match('^logservice_%s\\.\\d+\\.log$' % self.app_id, f)]
        self.log_files.sort(key=lambda x: int(re.match('^logservice_%s\\.(\\d+)\\.log$' % self.app_id, f).groups()[0]))
        log_file_index = 0
        for log_file in self.log_files:
            log_file_index += 1
            with open(os.path.join(self.factory.path, logfile), 'rb') as fh:
                position = 0
                while True:
                   buf = fh.read(4)
                   if not buf:
                       break
                   record_length = struct.unpack('I', buf)
                   buf = fh.read(record_length)
                   record = logging_capnp.RequestLog.from_bytes(buf)
                   self.indexRequestIds[record.requestId] = struct.pack('HI', log_file_index, position)
                   self.indexRequestTime[int(record.startTime)].write(struct.pack('dHI', record.startTime, log_file_index, position))

                
            



    def processActionLog(self, query):
        if not self.app_id:
            log.error("Don't know what to do, because the client did not set the app_id yet! Closing the connection ...")
            self.transport.loseConnection()
            return
    
    ACTIONS=dict(l=processActionLog, a=processSetAppId)


class LogServerFactory(protocol.Factory):
    protocol = Protocol

    def __init__(self, path, size):
        self.path = path
        self.size = size

