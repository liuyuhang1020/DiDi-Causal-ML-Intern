#!/usr/bin/env bash
# -*- coding: utf-8 -*-
set -e

echo "=== START ==="

# export PYSPARK_PYTHON=./python36/bin/python

spark-submit \
    --executor-cores 3 \
    --driver-memory 20g \
    --executor-memory 5g \
    --conf spark.default.parallelism=2400 \
    --conf spark.dynamicAllocation.maxExecutors=40 \
    --conf spark.driver.maxResultSize=5g \
    --conf spark.executor.memoryOverhead=3g \
    --conf spark.yarn.dist.archives=hdfs://DClusterNmg4/user/bigdata_driver_ecosys_test/qiuguolin/envs/python36.tgz#python36 \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./python36/bin/python \
    --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=/usr/local/hadoop-current/lib/native \
    --queue root.kg_novel_dev \
    --py-files __init__.py \
    __init__.py  

echo "=== END ==="