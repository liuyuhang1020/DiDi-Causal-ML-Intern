# 需要改为自己机器的地址，python3的环境。
export PYSPARK_PYTHON=/home/luban/anaconda3/envs/python36/bin/python

PARAM='{"is_online":"test","trace_id":"treat","order_id":2021090400,
"start_date":"2021-10-18","end_date":"2021-10-24","remove_date":[],
"city_list":[2,3,8,10,12,13,14,16,19,20,21,22,24,25,27,28,29,30,32,33,34,35,36,37,38,39,40,41,42,44,45,46,47,48,49,50,51,53,54,55,56,57,58,59,60,61,62,63,64,65,67,71,72,74,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373],
"zq_city_list":[],"budget":1068500000,"amount_type":"headquarters_budget",
"city_gmv_list":"","rate":0,"ext_data":"","task_type":"start_budget_decision","operator":"darwinadmin","trigger_time":"2021-10-11 16:47:00"}'
timeparts=${PARAM##*trigger_time}
DATE_DEF=${timeparts:3:10}
DATE=${2-"$DATE_DEF"}
CONFIG="./conf/job_config.json"
MODE=2
# echo "hadoop fs -test -e ly/bc_realloc_system/${DATE}/bc.csv"
# hadoop fs -test -e ly/bc_realloc_system/${DATE}/shumeng/bc.csv
# ret=$?
# if [ $ret -gt 0 ]; then
#     echo "preprocess"
#     MODE=2
# fi
#MODE=2
spark-submit \
    --executor-cores 3 \
    --driver-memory 6g \
    --executor-memory 13g \
    --conf spark.default.parallelism=2400 \
    --conf spark.dynamicAllocation.maxExecutors=40 \
    --conf spark.driver.maxResultSize=5g \
    --conf spark.yarn.dist.archives=hdfs://DClusterNmg4/user/bigdata_driver_ecosys_test/ly/python_env/python3.6.2.tgz#python3.6.2 \
    --conf spark.pyspark.driver.python=/home/luban/anaconda3/envs/python36/bin/python3 \
    --conf spark.pyspark.python=./python3.6.2/bin/python \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./python3.6.2/bin/python \
    --queue root.kg_novel_dev \
    --py-files bc_strategy.py,utils.py,hive_sql.py \
    generate_bc_strategy.py   --config "${CONFIG}" --date "${DATE}" --parameter="${PARAM}" --mode="${MODE}"


# --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./python3.6.2/bin/python 需要改为自己机器的地址，python3的环境。
