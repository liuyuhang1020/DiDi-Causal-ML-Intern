import json
import requests
import pandas as pd
import os
import tempfile
import logging
from datetime import datetime, timedelta
from jsonschema import Draft3Validator

main_path = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(format='[%(asctime)s]: %(message)s', level=logging.DEBUG)
logger = logging.getLogger()
# TODO
hadoop_queue = "root.pricing_driver_prod"


def execute_cmd(cmd, raise_flag=True):
    logger.info(f"executing command:\n{cmd}")
    ret = os.system(cmd)
    if raise_flag and ret != 0:
        raise Exception(f'Failed to execute command: {cmd}')
    return ret


def hdfs_exist(path):
    cmd = f"hdfs dfs -test -e {path}"
    ret = execute_cmd(cmd, False)
    return ret == 0


def execute_hive_sql(sql):
    _, output = tempfile.mkstemp(prefix='smt_', dir=main_path)
    cmd = f'hive --hiveconf mapreduce.job.queuename={hadoop_queue} -e "\n{sql}\n" > {output}\n'
    execute_cmd(cmd)
    return pd.read_csv(output, sep="\t")


def upload_to_hive(df, columns, table, partition_date, external_partition=''):
    """

	@param df:
	@param columns:
	@param table:
	@param partition_date: str
	@param external_partition:
	@return:
	"""
    _, filename = tempfile.mkstemp(prefix='smt_', dir=main_path)
    print(f"dumps to file {filename}")
    df[columns].to_csv(filename, sep='\t', index=None, header=None)
    cmd = f'''
	hive --hiveconf mapreduce.job.queuename={hadoop_queue} -e "load data local inpath '{filename}' overwrite into table {table} 
	partition(dt='{partition_date}'{'' if external_partition == '' else ','}{external_partition});"
	'''
    print(cmd)
    ret = execute_cmd(cmd)
    if ret == 0:
        simple_external_partition = external_partition.replace('\'', '').split(',')
        url = f"http://10.74.113.54:8021/stg/data_tag?partition={partition_date}&table={table}{'%23'.join([''] + simple_external_partition)}"
        resp = requests.get(url)
        print_log(f"sending make tag request, url: {url}, response: {resp.text}")


def drop_partition(table, partition_date, external_partition=''):
    partition_date = datetime.strptime(partition_date, "%Y-%m-%d")
    cmd = f'''
hive --hiveconf mapreduce.job.queuename={hadoop_queue} -e "
ALTER TABLE {table} DROP PARTITION(
	year='{partition_date.year:04d}',month='{partition_date.month:02d}',day='{partition_date.day:02d}'{'' if external_partition == '' else ','}{external_partition}
);"
	'''
    # logger.info(cmd)
    ret = execute_cmd(cmd)
    if ret == 0:
        simple_external_partition = external_partition.replace('\'', '')
        url = f"http://10.74.113.54:8021/stg/data_tag?partition={partition_date.strftime('%Y-%m-%d')}&del=1&table={table}{'' if external_partition == '' else '%23'}{simple_external_partition}"
        resp = requests.get(url)
        print_log(f"sending delete tag request, url: {url}, response: {resp.text}")


def print_log(msg):
    logger.info(msg)


# print(msg)


def send_message(title, msg, webhook=""):
    if webhook == "":
        webhook = "https://im-dichat.xiaojukeji.com/api/hooks/incoming/839e4cbe-2c7a-4db0-b918-d1d0e0f8ff46"
    # webhook = "https://im-dichat.xiaojukeji.com/api/hooks/incoming/cf4d7cd5-dfe8-4c15-92a5-e88d9202fd8f"
    robot = DChatRobot(webhook)
    robot.send_message(
        f'【预算分配-{title}】',
        [{"text": msg}],
        False
    )


class DChatRobot(object):
    """DChatRobot"""
    webhook = ""

    def __init__(self, webhook):
        super(DChatRobot, self).__init__()
        self.webhook = webhook

    def send_message(self, title, content_list, is_md=True):
        data = {
            "text": title,
            "markdown": is_md,
            "attachments": content_list
        }
        return self.post(data)

    def post(self, data):
        post_data = json.dumps({
            "web_hook": self.webhook,
            "data": data
        })
        print(post_data)
        HEADERS = {"Content-Type": "application/json ;charset=utf-8 "}
        req = requests.post("http://10.74.113.54:8021/stg/dchat_notification", post_data, headers=HEADERS)


fence_dic = {14: {3908418, 3908419, 3908420},
             7: {3908384, 3908385, 3908386, 3908387, 3908388},
             90: {3908438, 3908439, 3908440, 3908441},
             86: {3887691, 3887692, 3887693, 3887694, 3887695, 3887696},
             156: {3908485, 3908486},
             138: {3908470, 3908471},
             35: {3908415, 3908416, 3908417},
             50: {3908459, 3908460},
             256: {3908483, 3908484},
             133: {3735943, 3735944, 3887697, 3887698},
             20: {3908409, 3908410, 3908411, 3908412, 3908413, 3908414},
             140: {3908479, 3908480},
             160: {3888024, 3888025},
             39: {3908455, 3908456},
             81: {3908450, 3908451, 3908452, 3908453, 3908454},
             92: {3908442, 3908443, 3908444, 3908445, 3908446},
             34: {3908421, 3908422, 3908423},
             # 3: {3887684, 3887685, 3887686, 3887687},
             44: {3888026, 3888027, 3888028, 3888029, 3888030},
             23: {3908389, 3908390, 3908391, 3908392, 3908393, 4893998},
             1: {3908365, 3908366, 3908367, 5393057},
             62: {3908430, 3908431, 3908432, 3908433},
             12: {3908424, 3908425},
             306: {3908487, 3908488, 3908489, 3908490, 3908491},
             13: {3908405, 3908406, 3908407, 3908408},
             93: {3908476, 3908477, 3908478},
             85: {3887688, 3887689, 3887690},
             95: {3908463, 3908464, 3908465},
             255: {3908492, 3908493},
             36: {3908398, 3908399, 3908400, 3908401},
             173: {3908472, 3908473, 3908474, 3908475},
             17: {3908370, 3908371, 3908372, 3908373, 3908374, 3908375, 3908376, 3908377},
             153: {3735945, 3735946, 3735947, 3735948, 3735949},
             46: {3908434, 3908435, 3908436, 4893999},
             89: {3908426, 3908427, 3908428, 3908429},
             154: {3888021, 3888022},
             11: {389247, 389255},
             157: {3908447, 3908448, 3908449},
             40: {3908468, 3908469},
             45: {3735962, 3735963, 3735964},
             29: {3887713, 3887714, 3887737, 3887827},
             87: {3887699, 3887700, 3887701, 3887702},
             9: {501087},
             4: {3908368, 3908369},
             254: {3908466, 3908467},
             47: {3782765, 3782766, 3782767},
             24: {3782080, 3782152, 5190017, 4894005},
             88: {3887703, 3887704, 3887705, 3887706, 3887707, 3887708},
             105: {3956944, 3956945},
             143: {3887908, 3887955},
             53: {3888031, 3888032},
             19: {5393058, 3782210, 3782261},
             79: {3908481, 3908482},
             159: {3908457, 3908458},
             158: {3887709, 3887710},
             106: {3908461, 3908462},
             18: {62828, 145820},
             # 5: {3908378, 3908379, 3908380, 3908381, 3908382, 3908383},
             84: {3887711, 3887712}}


def validate_tasks(df, product_line):
    """

    @param df:
    @param product_line:
    @return:
    """
    cols = ["city_id", "fence_id", "start_time", "end_time", "cr", "gmv", "budget"]
    for col in cols:
        if col not in df.columns:
            return f"数据列存在问题，必须是 {','.join(cols)}"

    if df[(df.fence_id <= 0) & (df.fence_id != -1)].shape[0]:
        return f"存在非法fence_id。fence_id小于0时只能为-1"

    def parse_time(x):
        try:
            y = datetime.strptime(x, "%H:%M:%S")
            return True
        except:
            return False

    if df[~df["start_time"].map(parse_time)].shape[0] != 0:
        return f"存在非法start_time。start_time必须按照 XX:YY:ZZ的格式"
    if df[~df["end_time"].map(parse_time)].shape[0] != 0:
        return f"存在非法end_time。end_time必须按照 XX:YY:ZZ的格式"

    if df[(df.budget <= 100) & (df.budget != 0)].shape[0] > 0:
        return f"存在非法budget。budget必须大于100"

    if df[["city_id", "fence_id", "start_time", "end_time"]].drop_duplicates().shape[0] != df.shape[0]:
        return f"存在重复的活动时段"
    if product_line != 'kuaiche':
        return ""
    valid_fence_ids = [
        -1, 4894005, 3887692, 3908474, 3908477, 3887694, 3908406, 3908393, 3887710, 3908475, 3908420, 3887702,
        3887700, 3908487, 3908440, 3908370, 3908457, 3908388, 3887705, 3908401, 3888030,
        3887701, 3908492, 3908375, 3782261, 3908366, 3888021, 3908418, 3908392, 3887908, 5190017, 3887709, 3908488,
        3908384, 3908436, 3908432, 145820, 3908479, 3908423, 3887827, 3908454, 3908427, 3782765, 3908480,
        3908371, 3735949, 3908450, 3908466, 3782080, 3908458, 3908448, 3908389, 3908428, 3887697, 3782766,
        3908444, 3908470, 3887714, 5393057, 3908476, 3908422, 3908461, 3888026, 3908483, 3908419, 3908445, 3908449,
        3908484, 3908410, 3888022, 62828, 3908471, 5393058, 3908481, 3887703, 3956944, 3908447, 3908453, 3887706,
        3887688, 3735945, 3908412, 3908373, 3908425, 3735947, 3908424, 3908442, 3735963, 3908415, 3908372,
        3887737, 3908459, 3908398, 3908433, 3887711, 3735964, 3908386, 3887690, 3735943, 3908434,
        3908390, 3887707, 3908438, 3888028, 3908369, 3887955, 3908429, 3908421, 3908490, 3908472, 3908416,
        3887712, 3908451, 3887699, 3908399, 3908455, 3888032, 3908460, 3908473, 3908377, 3908462, 3888031, 3908376,
        3735962, 3908414, 3887696, 3782152, 3735944, 3887693, 3908446, 3908463, 3908411, 3888027, 3908468, 3908407,
        3908464, 3908441, 4893998, 3908485, 3735946, 3908489, 3908385, 3908467, 3887698, 3908368, 3887689,
        3908493, 3887713, 3887704, 4893999, 3908413, 3887695, 3956945, 3888024, 3908365, 3908408, 3908452,
        3908465, 3908443, 3908439, 3887708, 3908478, 3908469, 3908430, 3908391, 3735948, 3908387,
        3908435, 3782767, 3908482, 3888029, 3908491, 3908431, 3908426, 3888025, 3908374, 3908456, 3908409,
        3887691, 3908405, 3908417, 3908486, 3908400, 501087, 389247, 389255, 3782210, 3908367
    ]
    if df[~df.fence_id.isin(valid_fence_ids)].shape[0] != 0:
        return f"存在非法围栏 {set(df.fence_id.unique()) - set(valid_fence_ids)}，只能使用算法侧输出的围栏"

    def valid_fence(r):
        if r.city_id not in fence_dic and r.fence_id != -1:
            return False
        if r.city_id in fence_dic and r.fence_id not in fence_dic[r.city_id]:
            return False
        return True

    if df[~df.apply(valid_fence, axis=1)].shape[0] != 0:
        return f"存在非法围栏"
    return ""


def get_last_partition(table_name):
    """
	得到最新的分区
	@param table_name:
	@return:
	"""
    sql = f"""show partitions {table_name}"""
    df = execute_hive_sql(sql)
    partition_all = set()
    for part in df.partition:
        dt = part[5:9] + '-' + part[16:18] + '-' + part[23:25]
        partition_all.add(dt)
    return max(partition_all)


def gmv_limit(val):
    """

	@param val:
	@return:
	"""
    return round(float(val), 1) if val > 0 else 1.0


def validate_input(argument):
    """
	入参校验
	@param argument:
	@return:
	"""
    if not argument:
        raise Exception(f'INPUT ERROR, input params is {argument}')
    print(type(argument))
    print(argument)
    argu = json.loads(argument)
    argu['rely_info'] = json.loads(argu['rely_info'])
    if not argu['artificial']:
        argu['artificial'] = []
    if not argu['stg_constrain']['city_day_budget_rate_limit_list']:
        argu['stg_constrain']['city_day_budget_rate_limit_list'] = []
    print(argu)
    with open("./base_input_schema.json") as f:
        BASE_SCHEMA = json.load(f)
    BASE_VALIDATOR = Draft3Validator(BASE_SCHEMA)
    BASE_VALIDATOR.validate(argu)
    return argu


def call_back(url, data):
    """
	回调数据
	@param url:
	@param data:
	@return:
	"""
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    for i in range(1):
        try:
            ret = requests.post(url, data, headers=headers)
            result = json.loads(ret.text)
            if result['errno'] == 0:
                print('Succed call back')
                return True
            else:
                print(f'fail call back {i}: {result["errmsg"]}')
        except Exception as e:
            print(f'fail call back {i}: {e}')
    return False


def get_minute30_index(df, date_time):
    """
    时间片和索引转换
    @param df:
    @param date_time:
    @return:
    """
    hour, minute, _ = df[date_time].strip().split(' ')[-1].split(':')
    return int(hour) * 2 + round(int(minute) / 30)
