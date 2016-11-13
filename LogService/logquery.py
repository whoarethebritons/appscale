#!/usr/bin/python
import argparse

parser = argparse.ArgumentParser(description='Query AppScale logserver.')
parser.add_argument('--con', type=str, nargs='?', default='unix:///tmp/.appscale_logserver', help='Connection eg tcp://10.10.10.10:1010. (Default local log server)')
parser.add_argument('--start', type=int, nargs='?', help='start epoch timestamp')
parser.add_argument('--end', type=int, nargs='?', help='end epoch timestamp')
parser.add_argument('--ids', type=str, nargs='+', help='requestIds')
args = parser.parse_args()
print 'Connection:', args.con
print 'Start:', args.start
print 'End:', args.end
print 'Ids:', args.ids
