#!/usr/bin/env bash

DATE_DEF=`date -d"+0 day" +%Y-%m-%d`
DATE=${1-"$DATE_DEF"}
curhour=$2
# 1 xman  0 shumeng
MODE=0
PARAM='{"is_online":"online","trace_id":"123456","order_id":0,"zq_city_list":[],"remove_date":[],"city_list":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,359,360,361,362,363,364,365,366,367,368,369,370,371,372],"budget":1959300000,"task_type":"start_budget_decision","operator":"darwinadmin"}'

echo $PARAM

cp -f conf/job_config.json ./
CONFIG="job_config.json"
#python generate_c_strategy.py --config "${CONFIG}" --date "${DATE}" --parameter="${PARAM}"

export HADOOP_USER_PASSWORD=42gdaTh6voXDV0mR2glhNPjWqBxYe22N
export HADOOP_USER_NAME=bigdata_driver_ecosys_test
export PYSPARK_PYTHON=./python3.6.2/bin/python

    # --master yarn \
    # --deploy-mode client \

queue=root.celuemoxingbu_driver_service_score
jobnum=`yarn application --list|grep $queue|grep -v "_orig"|wc -l`
if [ $jobnum -lt 10 ];
then
queue=root.celuemoxingbu_driver_service_score_orig
fi

if [ 5 -lt $curhour ];
then
queue=root.kg_novel_dev
fi
# zip -r conf.zip conf

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --executor-cores 3 \
    --files job_config.json \
    --driver-memory 6g \
    --executor-memory 22g \
    --conf spark.yarn.executor.memoryOverhead=6144 \
    --conf spark.default.parallelism=2400 \
    --conf spark.dynamicAllocation.maxExecutors=40 \
    --conf spark.driver.maxResultSize=5g \
    --conf spark.yarn.dist.archives=hdfs://DClusterNmg4/user/bigdata_driver_ecosys_test/ly/python_env/python3.6.2.tgz#python3.6.2 \
    --conf spark.pyspark.python=./python3.6.2/bin/python \
    --queue ${queue} \
    --py-files bc_strategy.py,utils.py,hive_sql.py \
    generate_bc_strategy.py --config "${CONFIG}" --date "${DATE}" --parameter="${PARAM}" --mode="${MODE}"
