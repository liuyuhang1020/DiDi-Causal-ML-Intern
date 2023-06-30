import sys
sys.path.append("../")
from abc import abstractmethod
import pandas as pd
import numpy as np
import json
import logging
import re
import requests
import os
import tempfile
logger = logging.getLogger(__name__)
from datetime import datetime, timedelta
import argparse

from common.spark_client import SparkClient
from common.hive_client import HiveClient
from common.utils import get_14_days_before, get_city_hour_tbl, dic_to_str, check_partition

from data import Data
from model import Model
from budget import Budget
from copy_partition import CopyPartition


main_path = os.path.dirname(os.path.abspath(__file__))
hadoop_queue = "root.pricing_driver_prod"
table = "prod_smt_stg.symphony_budget_pitfill"


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

def upload_to_hive(df, columns, table, partition_date, external_partition=''):
    partition_date = datetime.strptime(partition_date, "%Y-%m-%d")
    _, filename = tempfile.mkstemp(prefix='smt_', dir=main_path)
    logger.info(f"dumps to file {filename}")
    df[columns].to_csv(filename, sep='\t', index=None, header=None)
    cmd = f'''
hive --hiveconf mapreduce.job.queuename={hadoop_queue} -e "
load data local inpath '{filename}' overwrite into table {table}
partition(
    year='{partition_date.year:04d}',month='{partition_date.month:02d}',day='{partition_date.day:02d}'{',' if external_partition != '' else ''}{external_partition}
);"
    '''
    # logger.info(cmd)
    ret = execute_cmd(cmd)
    if ret == 0:
        simple_external_partition = external_partition.replace('\'','')
        url = f"http://10.74.113.54:8021/stg/data_tag?partition={partition_date.strftime('%Y-%m-%d')}&table={table}{'%23' if external_partition != '' else ''}{simple_external_partition}"
        resp = requests.get(url)
        print(f"sending make tag request, url: {url}, response: {resp.text}")

def drop_partition(table, partition_date, external_partition=''):
    partition_date = datetime.strptime(partition_date, "%Y-%m-%d")
    cmd = f'''
hive --hiveconf mapreduce.job.queuename={hadoop_queue} -e "
ALTER TABLE {table} DROP PARTITION(
    year='{partition_date.year:04d}',month='{partition_date.month:02d}',day='{partition_date.day:02d}'{',' if external_partition != '' else ''}{external_partition}
);"
    '''
    ret = execute_cmd(cmd)
    if ret == 0:
        simple_external_partition = external_partition.replace('\'','')
        url = f"http://10.74.113.54:8021/stg/data_tag?partition={partition_date.strftime('%Y-%m-%d')}&del=1&table={table}{'%23' if external_partition != '' else ''}{simple_external_partition}"
        resp = requests.get(url)
        print(f"sending delete tag request, url: {url}, response: {resp.text}")



class Strategy(object):
    def __init__(self, conf, date, param, diagnosis_date):
        self._conf = conf
        self._date = date
        self._param = param
        self._diagnosis_date = diagnosis_date
    
    def fill_budget(self):
        sql = f"""
select
    result_date as pre_date,
    city_id,
    strategy_start as start_time,
    strategy_end as end_time,
    if(fence_id>0, fence_id, -1) as fence_id,
    fence_grid_list,
    (case 
        when (city_week_thre-frame_wdl)/(1.2*frame_wdl) < 0.02 then 0.02
        when (city_week_thre-frame_wdl)/(1.2*frame_wdl) > 0.12 then 0.12
        else (city_week_thre-frame_wdl)/(1.2*frame_wdl)
    end) * frame_gmv as budget,
    'B' as type
from prod_smt_dw.pangu_new_result_15d_v1
where concat_ws('-',year,month,day)='{self._diagnosis_date}'
    and strategy_type='fixed_threshold_0114_76'
    and frame_gmv>20000
        """
        df = execute_hive_sql(sql)
        columns = ["pre_date", "city_id", "start_time", "end_time", "fence_id", "fence_grid_list", "budget", "type"]

        drop_partition(table, self._date, "trace_id='123456'")
        upload_to_hive(df, columns, table, self._date, "trace_id='123456'")
        return df


    def generate(self):
        mask1, date1 = check_partition(self._conf['spark']['job_queue'],
                                       f"{self._conf['data']['DB']}.{self._conf['data']['budget_pitfill_tbl']}",
                                       self._date)
        print('symphony_budget_pitfill', mask1, date1)
        
        if mask1:
            print('copy partition!')
            cp = CopyPartition(self._conf, self._date, self._param).copy()
        else:   
            print('get data')
            df = self.fill_budget()
            return df


        
        