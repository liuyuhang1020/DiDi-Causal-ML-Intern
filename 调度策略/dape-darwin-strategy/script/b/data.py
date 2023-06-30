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

class Data(object):
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
    
    def get_fence_gmv(self):
        start = get_k_days_before(self._start, 21)
        end = get_k_days_before(self._end, 21)
        sql = """select
                    concat_ws('-', year, month, day) dt,
                    city_id as city_id,
                    fence_id as fence_id,
                    min30 as min30,
                    gmv as gmv,
                    tsh as tsh
                from prod_smt_stg.symphony_fence_gmv
                where concat_ws('-', year, month, day) between  '{start}' and '{end}'""".format(start=start, end=end)

        fence_gmv = execute_hive_sql(sql, self._conf['spark']['job_queue'])
        return fence_gmv
    
    def get_sunrise_diagnosis(self):
        sql = """select 
                      distinct
                      result_date dt,
                      cast(city_id as int) city_id,
                      cast(if(fence_id=0,-1,fence_id) as string) fence_id,
                      fence_grid_list fence_grid_list,
                      strategy_start start_time,
                      strategy_end end_time,
                      if(fence_fcnt>0,fence_fcnt,city_fcnt) fcnt,
                      frame_wdl wdl,
                      frame_gmv gmv
                from 
                      gulfstream.pangu_result_7d 
                where 
                      result_date between '{start}' and '{end}' 
                      and concat_ws('-',year, month, day) = '{dt}'""".format(start=self._start,
                                                                            end=self._end, 
                                                                            dt=self._date )
        zhenduan = execute_hive_sql(sql, self._conf['spark']['job_queue'])
        return zhenduan
    
    def filter_data(self, zhenduan):
        zhenduan = zhenduan[~zhenduan['dt'].isin(self._remove_date)][zhenduan['city_id'].isin(self._city_list)]
        return zhenduan
    
    def append_diagnosis_gmv(self, zhenduan):
        def shatter(line):
            start_idx = (60*int(line['start_time'][:2])+int(line['start_time'][3:5]))/30
            end_idx = (60*int(line['end_time'][:2])+int(line['end_time'][3:5]))/30
            if line['min30'] >=start_idx and  line['min30'] <end_idx:
                return 1
            else:
                return 0
            
        fence_gmv = self.get_fence_gmv()
        zhenduan['fence_id'] = zhenduan['fence_id'].apply(str)
        fence_gmv['fence_id'] = fence_gmv['fence_id'].apply(str)
        fence_gmv['dt'] = fence_gmv['dt'].apply(lambda dt: get_k_days_before(dt, -21))
        fence_gmv['gmv_fence'] = fence_gmv['gmv']
        fence_gmv = fence_gmv[['dt', 'city_id', 'fence_id', 'min30', 'gmv_fence']]
        zhenduan_gmv = zhenduan.merge(fence_gmv, on = ['dt', 'city_id', 'fence_id'], how = 'left')
        zhenduan_gmv['mask'] = zhenduan_gmv.apply(shatter, axis = 1)
        zhenduan_gmv = zhenduan_gmv[zhenduan_gmv['mask']>0].groupby(['dt', 'city_id', 'fence_id', 'start_time', 'end_time']).sum().reset_index()[['dt', 'city_id', 'fence_id', 'start_time', 'end_time','gmv_fence']]
        zhenduan['fence_id'] =  zhenduan['fence_id'].apply(str)
        zhenduan_gmv['fence_id'] = zhenduan_gmv['fence_id'].apply(str)
        zhenduan = zhenduan.merge(zhenduan_gmv, on = ['city_id', 'dt','fence_id', 'start_time', 'end_time'], how = 'left' )
        zhenduan['gmv_fence'] = zhenduan['gmv_fence'].fillna(0)
        return zhenduan 
    
    def process(self):
        city_diag = self.get_sunrise_diagnosis()
        city_diag = self.filter_data(city_diag)
        data = self.append_diagnosis_gmv(city_diag)
        print('get data over')
        return data