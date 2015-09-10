#!/bin/bash

cd results
for dir in `ls`; do
    cd $dir
    echo $dir
#    python ../../readFlowTrace.py rack-scale_plain rack-scale_plain_flows.txt
#    python ../../readFlowTrace.py rack-scale_combined rack-scale_combined_flows.txt
#    python ../../readFlowTrace.py rack-scale_timeonly rack-scale_timeonly_flows.txt
    python ../../readFlowTrace.py res-based_plain res-based_plain_flows.txt
#    python ../../readFlowTrace.py res-based_combined res-based_combined_flows.txt
    python ../../readFlowTrace.py res-based_timeonly res-based_timeonly_flows.txt
#    python ../../readFlowTrace.py nic nic_flows.txt
    cd ..
done
