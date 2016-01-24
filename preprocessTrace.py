#!/usr/bin/python3

import sys
import os
import subprocess

tracedir = sys.argv[1]
destdir = sys.argv[2]

print tracedir
print destdir

files = filter(lambda t: ("-disk-" not in t) or (t.endswith(".blktrace.0")), os.listdir(tracedir))
metas = filter(lambda t: ("-meta-" in t), files)
offsets = {t[0]: float(open("{}/{}".format(tracedir, t), 'r').readline()) / 1e6 for t in metas}
print offsets
subprocess.call("mkdir -p {}".format(destdir), shell=True)
for t in files:
    print("{}/{}".format(tracedir,t))
    if t == 'addr_mapping.txt' or t == 'traceinfo.txt':
        cmd = "cp {0}/{2} {1}/{2}".format(tracedir, destdir, t)
        print(cmd)
        subprocess.call(cmd, shell=True)

    elif "-nic-" in t:
        cmd = "cat {0}/{2} >> {1}/nic".format(tracedir, destdir, t)
        print cmd
        subprocess.call(cmd, shell=True)

    elif "-mem-" in t:
        # memory format: <ts> <<offset> ...>
        # cmd = "awk '{printf \"" + t[0] + " mem %.6f substr($0, index($0,$2))\\n\", $1}' " + "{}{}/{} >> ".format(tracedir, f, t) + "{}{}/trace".format(destdir, f)
        # memory format: <batch id> <ts> <addr start> <length>
        cmd = "awk '{printf \"" + t[0] + " mem %s\\n\", $0}' " + "{}/{} >> ".format(tracedir, t) + "{}/trace".format(destdir)
        print(cmd)
        subprocess.call(cmd, shell=True)

    elif "-disk-" in t:
        cmd = "blkparse {}/{} | egrep -v 'python|tcpdump|blktrace|cat|swap|bash|sh|auditd|kworker|crond' | python get_disk_io.py ".format(tracedir, t) + "| awk '{printf \"" + t[0] + " disk %.6f %s %s %s %s \\n\", $2+" + "{:f}".format(offsets[t[0]]) + ", $4, $6, $8, $10}' >> " + "{}/trace".format(destdir)
        print(cmd)
        subprocess.call(cmd, shell=True)
