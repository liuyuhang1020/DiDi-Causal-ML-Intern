import sys
sys.path.append("../")
from common.spark_client import SparkClient
from common.hive_client import HiveClient
from common.utils import get_14_days_before, get_city_hour_tbl, dic_to_str, execute_hive_sql
from abc import abstractmethod
import pandas as pd
import numpy as np
import json
import logging
import re
import requests
logger = logging.getLogger(__name__)
import datetime

class Model(object):
    def __init__(self, conf, date, param):
        self._conf = conf
        self._date = date
        self._start = param['start_date']
        self._end = param['end_date']
        self._trace_id = param['trace_id']
        self._remove_date = param['remove_date']
        self._city_list = param['city_list']
        self._spark_client = SparkClient(conf, "model")
        self._hive_client = HiveClient(conf) 
        
    def get_v3p1(self):
        sql = """select
                     pre_date
                    ,city_id
                    ,start_time
                    ,end_time
                    ,fence_id
                    ,fence_grid_list
                    ,predictions
               from gulfstream.sinan_tiankeng_pangu_dtsh_estimate_process
               where 
                    dt = '{dt}'""".format(dt = self._date)
        prediction = execute_hive_sql(sql, self._conf['spark']['job_queue'])
        prediction['fence_id'] = prediction['fence_id'].apply(lambda x: -1 if x == 0 else x).apply(str)
        prediction['dt'] = prediction['pre_date']
        return prediction
    
    
    def predict(self, data):
        
        def shatter(dic, lower_bound, upper_bound):
            res = {}
            for k in dic:
                if k >= lower_bound and k <= upper_bound:
                    res[k] = dic[k]
            return res
                    
        prediction = self.get_v3p1()
        data = data.merge(prediction[['city_id', 'start_time', 'end_time', 'fence_id', 'predictions', 'dt']],
                  on = ['dt', 'city_id', 'fence_id', 'start_time', 'end_time'], how = 'left')
        lower_bound = self._conf['optimise']['lower_bound']
        upper_bound = self._conf['optimise']['upper_bound']
        data['candidate_dic'] = data['predictions'].apply(eval).apply(lambda dic:shatter(dic,lower_bound,upper_bound))
        return data