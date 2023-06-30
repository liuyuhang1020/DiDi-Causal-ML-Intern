#!/usr/bin/env bash

set -x
python3Venv=/home/xiaoju/dape/python/stg3/bin/activate
basepath=$(cd `dirname $0`; pwd)
cd $basepath
source $python3Venv
export PYTHONPATH=$basepath
echo $PYTHONPATH


PARAM=$1
timeparts=${PARAM##*trigger_time}
DATE_DEF=${timeparts:3:10}
DATE=${2-"$DATE_DEF"}

CONFIG="./conf/job_config.json"

export PYSPARK_PYTHON=./python3.6.2/bin/python

    # --master yarn \
    # --deploy-mode client \

queue=root.celuemoxingbu_driver_service_score
jobnum=`yarn application --list|grep $queue|grep -v "_orig"|wc -l`
if [ $jobnum -gt 10 ];
then
queue=root.kg_novel_dev
fi

spark-submit \
    --executor-cores 3 \
    --driver-memory 6g \
    --executor-memory 13g \
    --conf spark.default.parallelism=2400 \
    --conf spark.dynamicAllocation.maxExecutors=40 \
    --conf spark.driver.maxResultSize=5g \
    --conf spark.yarn.dist.archives=hdfs://DClusterNmg4/user/bigdata_driver_ecosys_test/ly/python_env/python3.6.2.tgz#python3.6.2 \
    --conf spark.pyspark.driver.python=/home/xiaoju/dape/python/stg3/bin/python \
    --conf spark.pyspark.python=./python3.6.2/bin/python \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./python3.6.2/bin/python \
    --queue ${queue} \
    --py-files taocan.py,sql_file.py \
    generate_taocan.py    --config "${CONFIG}" --date "${DATE}" --parameter="${PARAM}" 