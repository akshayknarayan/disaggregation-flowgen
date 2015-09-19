#!/bin/bash

cd results
for dir in `ls`; do
    if [ "$dir" == "Readme.md" ]; then continue; fi
    cd $dir
    echo $dir
#    python ../../readFlowTrace.py res-based_plain res-based_plain_flows.txt
    python ../../readFlowTrace.py res-based_timeonly res-based_timeonly_flows.txt
#    python ../../readFlowTrace.py nic nic_flows.txt
    cd ..
done
curl -X POST "https://maker.ifttt.com/trigger/call_me/with/key/cTyEB1Uga6onvmR6HioIs-" > /dev/null 2> /dev/null
