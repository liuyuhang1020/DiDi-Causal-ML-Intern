export PYSPARK_PYTHON=/home/luban/anaconda3/envs/python36/bin/python
# export PYSPARK_PYTHON=/home/luban/anaconda3/bin/python
PARAM='{"is_online":"test","trace_id":"0a607334619dfed43e7dad72658221b0","order_id":12319,"start_date":"2021-11-17","end_date":"2021-11-23","remove_date":["2021-11-22"],"city_list":[3,19, 20, 21, 24, 33, 36, 38, 39, 46, 47, 48, 50, 53, 54, 58, 62, 79, 81, 82, 83, 88, 90, 91, 92, 93, 94, 95, 97, 105, 106, 133, 134, 135, 136, 138, 154, 157, 159, 160, 173, 253, 254, 283, 51, 52, 55, 56, 57, 59, 63, 67, 72, 76, 78, 98, 100, 101, 104, 109, 110, 112, 113, 115, 121, 125, 127, 144, 146, 147, 148, 151, 152, 166, 170, 174, 180, 181, 182, 184, 185, 186, 188, 196, 198, 199, 219, 220, 221, 222, 223, 227, 228, 229, 231, 232, 233, 234, 235, 236, 237, 238, 241, 244, 245, 246, 248, 250, 251, 255, 258, 259, 261, 262, 263, 266, 267, 270, 271, 272, 273, 280, 281, 282, 284, 286, 287, 289, 295, 297, 298, 299, 300, 302, 305, 306, 310, 311, 314, 317, 319, 331, 351, 360, 372],"zq_city_list":[18],"budget":100000000,"amount_type":"headquarters_budget_roi","city_gmv_list":"","rate":0,"ext_data":"{\"balance_mode\":\"\",\"dict_id\":\"\",\"shield_stg\":null,\"black_dict\":[]}","task_type":"start_budget_decision","operator":"darwinadmin","trigger_time":"2021-11-17 16:59:00","blackdict":[]}'
timeparts=${PARAM##*trigger_time}
DATE_DEF=${timeparts:3:10}
DATE=${2-"$DATE_DEF"}
CONFIG="./conf/job_config.json"

spark-submit \
    --executor-cores 3 \
    --driver-memory 6g \
    --executor-memory 13g \
    --conf spark.default.parallelism=2400 \
    --conf spark.dynamicAllocation.maxExecutors=40 \
    --conf spark.driver.maxResultSize=5g \
    --conf spark.yarn.dist.archives=hdfs://DClusterNmg4/user/bigdata_driver_ecosys_test/ly/python_env/python3.6.2.tgz#python3.6.2 \
    --conf spark.pyspark.driver.python=/home/luban/anaconda3/envs/python36/bin/python \
    --conf spark.pyspark.python=./python3.6.2/bin/python \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./python3.6.2/bin/python \
    --queue root.kg_novel_dev \
    --py-files taocan.py,sql_file.py \
    generate_taocan.py   --config "${CONFIG}" --date "${DATE}" --parameter="${PARAM}" 

