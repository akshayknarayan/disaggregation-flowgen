#!/usr/bin/python

import sys
import subprocess

traces = ['wordcount', 'graphlab', 'memcached', 'wordcount-hadoop', 'terasort']
# traces = ['memcached-smallnic']
template = 'bash -c "pypy makeFlowTrace.py results/{0}/ traces/{0}/*"'
nic_template = 'bash -c "pypy makeNicTrace.py results/{0}/ traces/{0}/*"'

if (len(sys.argv) > 1):
    print 'Usage: python makeTraces.py'
    sys.exit(1)

for trace in traces:
    subprocess.call(['mkdir', '-p', 'results/{}'.format(trace)])
    print template.format(trace)
    subprocess.call(template.format(trace), shell=True)
    # print nic_template.format(trace)
    # subprocess.call(nic_template.format(trace), shell=True)

# call me to let me know it's done.
subprocess.call("curl -X POST https://maker.ifttt.com/trigger/text_me/with/key/cTyEB1Uga6onvmR6HioIs- > /dev/null 2> /dev/null", shell=True)
