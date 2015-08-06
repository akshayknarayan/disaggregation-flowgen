#!/bin/bash

cd results
for dir in `ls`; do
    cd $dir
    echo $dir
    python ../../readFlowTrace.py delta1000_ delta1000_flows.txt
    python ../../readFlowTrace.py combined_ combined_flows.txt
    python ../../readFlowTrace.py flows.txt
    cd ..
done
