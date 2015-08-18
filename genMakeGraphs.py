#!/usr/bin/python

template = '''#!/bin/bash

cd results
for dir in `ls`; do
    cd $dir
    echo $dir
{}
    cd ..
done
'''

cmd_template = '    python ../../readFlowTrace.py {0}_{1} {0}_{1}_flows.txt'
cmd = []

for arch in ('rack-scale', 'res-based'):
    for comb_mode in ('plain', 'combined', 'timeonly'):
        cmd.append(cmd_template.format(arch, comb_mode))

cmds = '\n'.join(cmd)
with open('makeGraphs.sh', 'w') as f:
    f.write(template.format(cmds))
