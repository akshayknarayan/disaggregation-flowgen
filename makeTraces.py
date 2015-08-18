#!/usr/bin/python

import sys
import subprocess

traces = ['wordcount', 'graphlab', 'terasort', 'memcached']
template = 'bash -c "python makeFlowTrace.py results/{0}/ traces/{0}_with_nic/*"'

if (len(sys.argv) > 1):
    print 'Usage: python makeTraces.py'
    sys.exit(1)

for trace in traces:
    print template.format(trace)
    subprocess.call(template.format(trace), shell=True)
