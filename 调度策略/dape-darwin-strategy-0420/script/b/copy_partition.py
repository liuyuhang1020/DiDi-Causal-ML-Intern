import sys
sys.path.append("../")
from common.spark_client import SparkClient
from common.hive_client import HiveClient
from common.utils import get_k_days_before,get_city_hour_tbl,dic_to_str, execute_hive_sql
from abc import abstractmethod
import pandas as pd
import numpy as np
import json
import logging
import re
import requests
logger = logging.getLogger(__name__)
import datetime
import calendar


class CopyPartition:
    def __init__(self, conf, date, param):
        self._conf = conf
        self._date = date
        self._start = param['start_date']
        self._end = param['end_date']
        self._trace_id = param['trace_id']
        self._remove_date = param['remove_date']
        self._city_list = param['city_list']
        self._spark_client = SparkClient(conf, "data")
        self._hive_client = HiveClient(conf) 
    
    def filter_data(df):
        df = df[~df['pre_date'].isin(self._remove_date)][df['city_id'].isin(self._city_list)]
        return df
        
    def copy_budget_pitfill_tbl(self):
        sql = """select 
                    pre_date
                    ,city_id
                    ,start_time
                    ,end_time
                    ,fence_id
                    ,fence_grid_list
                    ,budget
                    ,type
        from {budget_pitfill_tbl}
        where concat_ws('-',year,month,day) = '{date}' 
        and trace_id = '123456'""".format(
            budget_pitfill_tbl = f"{self._conf['data']['DB']}.{self._conf['data']['budget_pitfill_tbl']}",
            date = self._date)
        data_pitfill = execute_hive_sql(sql, self._conf['spark']['job_queue'])
        self._hive_client.upload_to_partition(data_pitfill, 
                                         self._conf['data']['budget_pitfill_tbl_columns'], 
                                         self._date,
                                         f"trace_id='{self._trace_id}'",
                                         f"{self._conf['data']['DB']}.{self._conf['data']['budget_pitfill_tbl']}")
        print(data_pitfill.head(20))
        print('copy budget_pitfill_tbl finished!')
        
    def copy_model_predicitons_tbl(self):
        sql = """select 
                     pre_date
                    ,city_id
                    ,start_time
                    ,end_time
                    ,fence_id
                    ,fence_grid_list
                    ,predictions
        from {model_predicitons_tbl}
        where concat_ws('-',year,month,day) = '{date}' 
        and trace_id = '123456'""".format(
            model_predicitons_tbl = f"{self._conf['data']['DB']}.{self._conf['data']['model_predicitons_tbl']}",
            date = self._date)
        data_pred = execute_hive_sql(sql, self._conf['spark']['job_queue'])
        self._hive_client.upload_to_partition(data_pred, 
                                         self._conf['data']['model_predicitons_tbl_columns'], 
                                         self._date,
                                         f"trace_id='{self._trace_id}'",
                                         f"{self._conf['data']['DB']}.{self._conf['data']['model_predicitons_tbl']}")
        print('copy model_predicitons_tbl finished!')
    def copy(self):
        self.copy_budget_pitfill_tbl()
        #self.copy_model_predicitons_tbl()
        print('copy hive success!')