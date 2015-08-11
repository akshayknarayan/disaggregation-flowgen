#!/usr/bin/python

import sys
import subprocess

traces = ['wordcount', 'graphlab', 'terasort', 'memcached']
template = 'bash -c "python makeFlowTrace.py results/{0}/{1}flows.txt traces/{0}/*"'
nic_template = 'bash -c "python makeNicTrace.py results/{0}/{1}flows.txt traces/{0}_with_nic/*-nic-*"'

flavor = ''
if (len(sys.argv) > 1):
    flavor = sys.argv[1] + '_'
elif (len(sys.argv) > 2):
    print 'Usage: python makeTraces.py <optional thing to prepend to flow file name>'
    sys.exit(1)

for trace in traces:
    print nic_template.format(trace, flavor)
    subprocess.call(nic_template.format(trace, flavor), shell=True)

