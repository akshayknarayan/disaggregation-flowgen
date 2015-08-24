#!/usr/bin/python

import sys
import subprocess

traces = ['graphlab', 'terasort', 'memcached']
template = 'bash -c "./parseTrace results_hs/{0}/ -make n traces/{0}_with_nic/*"'
step2 = 'bash -c "./parseTrace results_hs/{0}/ -collapse timeonly results_hs/{0}/rack-scale_plain_flows.txt"'
step3 = 'bash -c "./parseTrace results_hs/{0}/ -collapse timeonly results_hs/{0}/res-based_plain_flows.txt"'

if (len(sys.argv) > 1):
    print 'Usage: python makeTracesHs.py'
    sys.exit(1)

for trace in traces:
    print template.format(trace)
    subprocess.call(template.format(trace), shell=True)
    subprocess.call(step2.format(trace), shell=True)
    subprocess.call(step3.format(trace), shell=True)
