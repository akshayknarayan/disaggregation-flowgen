#!/bin/bash

traces=("../expanded_traces/bdb/" "../expanded_traces/wordcount_hadoop/" "../expanded_traces/terasort_hadoop/" "../expanded_traces/wordcount_spark/" "../expanded_traces/terasort_spark/" "../expanded_traces/timely/" "../expanded_traces/memcached/")

for tr in ${traces[@]}
do
    echo $tr
    ../../spark/bin/spark-submit --class org.apache.spark.examples.ProcessTrace --properties-file ../../spark/conf/spark-defaults.conf ../../spark/examples/target/scala-2.11/spark-examples-1.5.1-hadoop2.4.0.jar 2000 $tr
done
