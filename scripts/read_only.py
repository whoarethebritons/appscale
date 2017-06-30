#!/usr/bin/env python2

""" This script turns read-only mode on or off for the entire deployment.

This changes the read-only status of all datastore servers and disables
(or re-starts) the groomer on all datastore nodes.

Args:
  A string containing 'on' or 'off'.
Returns:
  A string indicating whether or not the operation was a success.
"""

import sys

from appscale.common import appscale_info


if __name__ == '__main__':
  make_active = False
  if len(sys.argv) < 2 or sys.argv[1] not in ['on', 'off']:
    print('Please give a value of "on" or "off".')
    sys.exit(1)

  if sys.argv[1] == 'on':
    make_active = True

  acc = appscale_info.get_appcontroller_client()
  result = acc.set_read_only(str(make_active).lower())
  if result != 'OK':
    print(result)
    sys.exit(1)

  if make_active:
    print('Datastore writes are now disabled.')
  else:
    print('Datastore writes are now enabled.')
