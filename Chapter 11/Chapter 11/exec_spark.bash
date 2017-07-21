#!/bin/bash

spark-submit \
  --class $1 \
  --master spark://hc2nn.semtech-solutions.co.nz:7077  \
  --executor-memory 700M \
  --total-executor-cores 100 \
  /home/hadoop/spark/graphx/target/scala-2.10/graph-x_2.10-1.0.jar  \
  1000

