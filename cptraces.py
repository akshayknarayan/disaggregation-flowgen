#!/usr/bin/python

import subprocess
import sys

trace_dir = '/work/petergao/traces'
cp_cmd = 'cp -r {} {}'
awk_cmd = "awk -F['_'] '{print $1}'"

if (len(sys.argv) > 1):
    pat = sys.argv[1]
    files = subprocess.check_output("ls {} | grep {}".format(trace_dir, pat), shell=True)
    files = files.split('\n')
else:
    assert(False)
    files = None

for f in files[:-1]:
    localdir = subprocess.check_output("echo {} | {}".format(f, awk_cmd), shell=True)[:-1]
    print f, localdir
    subprocess.call(['cp', '-r', trace_dir + '/' + f, 'traces/' + localdir])
