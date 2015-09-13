#!/usr/bin/python

import sys
import subprocess

traces = ['wordcount', 'graphlab', 'memcached', 'wordcount-hadoop']
template = 'bash -c "pypy makeFlowTrace.py results/{0}/ traces/{0}/*"'
nic_template = 'bash -c "pypy makeNicTrace.py results/{0}/ traces/{0}/*"'

if (len(sys.argv) > 1):
    print 'Usage: python makeTraces.py'
    sys.exit(1)

for trace in traces:
    subprocess.call(['mkdir', '-p', 'results/{}'.format(trace)])
    print template.format(trace)
    subprocess.call(template.format(trace), shell=True)
    print nic_template.format(trace)
    subprocess.call(nic_template.format(trace), shell=True)
