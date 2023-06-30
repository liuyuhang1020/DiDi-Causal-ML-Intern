import os
import tempfile
import argparse
import json
import requests
import sys
import traceback
import pandas as pd
import time
import logging
import pandas as pd
from datetime import datetime, timedelta


main_path = os.path.dirname(os.path.abspath(__file__))
hadoop_queue = "root.pricing_driver_prod"
tomorrow = (datetime.now()+timedelta(1)).strftime("%Y-%m-%d")

logging.basicConfig(format='[%(asctime)s]:%(message)s', level=logging.DEBUG)
logger = logging.getLogger()


def execute_cmd(cmd):
    logger.info(f"executing command:\n{cmd}")
    ret = os.system(cmd)
    if ret != 0:
        raise Exception(f'Failed to execute command: {cmd}')
    return ret


def execute_hive_sql(sql):
    _, output = tempfile.mkstemp(prefix='smt_', dir=main_path)
    cmd = f'hive --hiveconf mapreduce.job.queuename={hadoop_queue} -e "\n{sql}\n" > {output}\n'
    execute_cmd(cmd)
    return pd.read_csv(output, sep="\t")


def get_vaild_city_list():
    data = {
        "amount_type": "headquarters_budget",
        "caller": "b",
        "product_line": "kuaiche",
        "stat_date": tomorrow,
        "sum_source": 1
    }
    res = requests.post("http://10.88.128.149:32234/darwin/amount/getamountrec", data=data)
    print(res.text)
    res = res.json()
    budget_list = res["data"]["list"]
    city_list = list(map(lambda x: x["city"], filter(lambda x: x['amount'] > 0, budget_list)))
    return city_list


def get_external_budget():
    res = requests.post("http://10.88.128.149:32234/darwin/decision/getdecisiondetail", data={"stat_date": tomorrow}).json()
    budget_list = res['data']['done_list']
    timestamp = list(filter(lambda x: x['caller']=='b', budget_list))[0]['update_time']
    time_array = time.localtime(timestamp)
    order_date = time.strftime("%Y-%m-%d", time_array)
    sql = f"""
select
    a.city_id as city_id,
    a.budget-nvl(b.budget, 0) as budget
from (
    select
        city_id,
        sum(budget) as budget
    from prod_smt_stg.symphony_budget_pitfill 
    where concat_ws('-',year,month,day)='{tomorrow}'
        and trace_id='123456' and type='B' and pre_date='{tomorrow}'
    group by city_id
) a left join (
    select
        city_id,
        sum(budget) as budget
    from prod_smt_stg.symphony_budget_pitfill 
    where concat_ws('-',year,month,day)='{order_date}'
        and trace_id='123456' and type='B' and pre_date='{tomorrow}'
    group by city_id
) b on a.city_id=b.city_id
where a.budget > nvl(b.budget, 0) and nvl(b.budget, 0)/a.budget < 0.7
    """
    # print(sql)
    return execute_hive_sql(sql)


def report_external_budget(df):
    lis = []
    for _, r in df.iterrows():
        lis.append({"city_id": int(r.city_id), "amount": int(r.budget*100)})
    req = {
        "stat_date": tomorrow,
        "city_amount": json.dumps({"city_amount_list": lis})
    }
    logger.info(req)
    res = requests.post("http://10.88.128.149:32234/darwin/amount/setweatherext", data=req).json()
    logger.info(res)


def main():
    city_list = get_vaild_city_list()
    logger.info(f"vaild city list: {city_list}")
    df = get_external_budget()
    if df.shape[0] == 0:
        logger.info(f"no external budget required, returned")
        exit(0)
    df = df[df.city_id.isin(city_list)]
    report_external_budget(df)  

if __name__ == '__main__':
    main()








