#!/usr/bin/env bash

set -ex
python2Venv=/home/xiaoju/dape/python/stg/bin/activate
basepath=$(cd `dirname $0`; pwd)
cd $basepath
source $python2Venv
export PYTHONPATH=$basepath
echo $PYTHONPATH
ret=$?

#DATE=`date -d"+0 day" +%Y-%m-%d`
PARAM=$1
CONFIG="./conf/job_config.json"

timeparts=${PARAM##*trigger_time}
DATE=${timeparts:3:10}

#python generate_c_strategy.py --config "${CONFIG}" --date "${DATE}" --parameter="${PARAM}"

export PYSPARK_PYTHON=./python2.7/bin/python
    #--master yarn \
    #--deploy-mode cluster \
    #--master local \
    #--queue root.celuemoxingbu_driver_service_score_orig \
#generate_c_strategy.py --config "${CONFIG}" --date "${DATE}" --parameter='{"is_online":false,"trace_id":"0a607334603f31bef0d199006cffa2b0","order_id":22,"start_date":"2021-03-06","end_date":"2021-03-07","remove_date":[""],"city_list":[1,2,4],"budget":100000000,"operator":"admin","trigger_time":"2021-03-03 14:50:38"}'
queue=root.celuemoxingbu_driver_service_score
jobnum=`yarn application --list|grep $queue|grep -v "_orig"|wc -l`
if [ $jobnum > 10 ];
then
queue=root.kg_novel_dev
fi

hdfspath=/user/bigdata_driver_ecosys_test/luowenjuan/testhiveout/c_budget_allocation_tmp/dt=${DATE}
hadoop fs -test -e ${hdfspath}
if [ $? -ne 0 ]; then
   spark-submit \
    --executor-cores 3 \
    --driver-memory 6g \
    --executor-memory 13g \
    --num-executors 100 \
    --conf spark.default.parallelism=2400 \
    --conf spark.dynamicAllocation.maxExecutors=400 \
    --conf spark.driver.maxResultSize=5g \
    --conf spark.yarn.dist.archives=hdfs://DClusterNmg4/user/bigdata_driver_ecosys_test/bolinlin/roi_predict/python2.7.tgz#python2.7 \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./python2.7/bin/python \
    --queue ${queue} \
    generate_c_strategy.py --config "${CONFIG}" --date "${DATE}" --parameter="${PARAM}"
else
    echo "C budget already generated."
fi

if [ $ret -eq 1 ]; then
    echo "run failed return $ret"
    deactivate
    exit $ret
fi
deactivate
