import sys
import re

for line in sys.stdin:
    lineArr = re.sub(' +', ' ', line.strip()).split(' ')
    if "," in lineArr[0]:
        major = int(lineArr[0].split(",")[0])
        minor = int(lineArr[0].split(",")[1])
        cpu = int(lineArr[1])
        seq = int(lineArr[2])
        ltime = float(lineArr[3])
        pid = int(lineArr[4])
        action = lineArr[5]
        rw = lineArr[6]
        if len(lineArr) >= 11:
            offset = lineArr[7]
            assert(lineArr[8] == "+")
            size = lineArr[9]
            proc = lineArr[10:]

            # action classifiers through IO event lifetime: Q G I D C
            # Q = queued, D = issue(d)/started, C = complete
            if action == "D":  # and proc[0] == "[java]":
                print "Time:", ltime, " Offset:", offset, " Size:", size, " Action:", action, " RW:", rw, " Proc:", proc
