#!/usr/bin/python3

import sys
import os
import subprocess

tracedir = sys.argv[1]
destdir = sys.argv[2]

for f in os.listdir(tracedir):
    print("{}{}/".format(tracedir, f))
    files = filter(lambda t: ("-disk-" not in t) or (t.endswith(".blktrace.0")), os.listdir("{}{}/".format(tracedir, f)))
    metas = filter(lambda t: ("-meta-" in t), files)
    offsets = {t[0]: float(open("{}{}/{}".format(tracedir, f, t), 'r').readline()) / 1e6 for t in metas}
    print offsets
    subprocess.call("mkdir -p {}{}".format(destdir, f), shell=True)
    for t in files:
        if t == 'addr_mapping.txt' or t == 'traceinfo.txt':
            #continue
            print("cp {0}{1}/{2} {3}{1}/{2}".format(tracedir, f, t, destdir))
            subprocess.call("cp {0}{1}/{2} {3}{1}/{2}".format(tracedir, f, t, destdir), shell=True)

        elif "-nic-" in t:
            #continue
            cmd = "cat {0}{1}/{2} >> {3}{1}/nic".format(tracedir, f, t, destdir)
            print cmd
            subprocess.call(cmd, shell=True)

        elif "-mem-" in t:
            continue
            # memory format: <ts> <<offset> ...>
            # cmd = "awk '{printf \"" + t[0] + " mem %.6f substr($0, index($0,$2))\\n\", $1}' " + "{}{}/{} >> ".format(tracedir, f, t) + "{}{}/trace".format(destdir, f)
            # memory format: <batch id> <ts> <addr start> <length>
            cmd = "awk '{printf \"" + t[0] + " mem %s\\n\", $0}' " + "{}{}/{} >> ".format(tracedir, f, t) + "{}{}/trace".format(destdir, f)
            print(cmd)
            subprocess.call(cmd, shell=True)

        elif "-disk-" in t:
            continue
            cmd = "blkparse {}{}/{} | egrep -v 'python|tcpdump|blktrace|cat|swap|bash|sh|auditd' | python get_disk_io.py ".format(tracedir, f, t) + "| awk '{printf \"" + t[0] + " disk %.6f %s %s %s %s \\n\", $2+" + "{:f}".format(offsets[t[0]]) + ", $4, $6, $8, $10}' >> " + "{}{}/trace".format(destdir, f)
            print(cmd)
            subprocess.call(cmd, shell=True)
