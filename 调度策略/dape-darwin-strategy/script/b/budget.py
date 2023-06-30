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
from datetime import datetime, timedelta

def data_process(df):
    hour = pd.DataFrame()
    hour['hour'] = range(24)
    df = df.merge(hour, on = 'hour', how = 'right')
    df['pred_date'] = df['pred_date'].fillna(df['pred_date'].dropna().unique()[0])
    if len(df['model_params'].dropna().values) > 0:
        params = df['model_params'].dropna().unique()[0]
    else:
        params = '-1,0.0,0.0,0.0'
    df['model_params'] = df['model_params'].fillna(params)
    df['supply'] = df['supply'].fillna(0)
    df['demand'] = df['demand'].fillna(0)
    return df[['pred_date', 'hour', 'model_params', 'supply', 'demand']]



class Budget(object):
    def __init__(self, conf, date, param):
        self._conf = conf
        self._date = date
        self._start = param['start_date']
        self._end = param['end_date']
        self._trace_id = param['trace_id']
        self._remove_date = param['remove_date']
        self._city_list = param['city_list']
        self._spark_client = SparkClient(conf, "budget")
        self._hive_client = HiveClient(conf) 
        
    def get_fr_model_params(self):
        print('get_model_params.sql')
        dt = (datetime.strptime(self._date, "%Y-%m-%d") - timedelta(1)).strftime("%Y-%m-%d")
        sql = """select
                    city_id
                    ,pred_date
                    ,hour
                    ,supply
                    ,demand
                    ,model_params
                from prod_smt_stg.symphony_answer_rate_hourly
                where concat_ws('-', year, month, day) = '{dt}'""".format(dt=dt)
        model_params = execute_hive_sql(sql, self._conf['spark']['job_queue'])
        model_params = model_params.groupby(['city_id', 'pred_date', 'hour']).apply(lambda rows: pd.Series({
            'model_params': rows['model_params'].values[0],
            'supply': rows['supply'].sum(),
            'demand': rows['demand'].sum()
        })).reset_index()

        model_params = model_params.groupby('city_id').apply(data_process).reset_index()
        model_params['city_id'] = model_params['city_id'].apply(int)
        model_params['pred_date'] = model_params['pred_date'].apply(str)
        model_params['hour'] = model_params['hour'].apply(int)
        fr_model_param_dict = {}
        for city_id in range(0,400):
            if city_id in model_params['city_id'].unique():
                string = model_params[(model_params['city_id'] == city_id)]['model_params'].unique()[0]
                sd_coef = float(re.split(r',', string)[0])
                supply = model_params[model_params['city_id'] == city_id].sort_values('hour', ascending=True)['supply'].values
                demand = model_params[model_params['city_id'] == city_id].sort_values('hour', ascending=True)['demand'].values
                fr_model_param_dict[city_id] = {'sd_coef': sd_coef, 'supply': supply, 'demand': demand}
            else:
                fr_model_param_dict[city_id] = {'sd_coef': -1, 'supply': 100, 'demand': 100}      
        return fr_model_param_dict
    
    
    def upload_pitfill_budget(self, data):
        data['pre_date'] = data['dt']
        table = f"{self._conf['data']['DB']}.{self._conf['data']['budget_pitfill_tbl']}"
        self._hive_client.upload_to_partition(data, 
                                             self._conf['data']['budget_pitfill_tbl_columns'], 
                                             self._date,
                                             f"trace_id='{self._trace_id}'",
                                             table)

        url = f"http://10.74.113.54:8021/stg/data_tag?partition={self._date}&table={table}%23trace_id={self._trace_id}"
        resp = requests.get(url)
        print(f"sending delete tag request, url: {url}, response: {resp.text}")

    def fill_budget_0(self, df):
        #fill the budget of activities with fr_rate >0.72 
        def fr_to_sr(fr):
            #{'fr_rate':0.72:'sub_ratio':0.03}, {'fr_rate':0.85,'sub_ratio':0.01}
            if fr <= 0.72:
                return 0.03
            elif fr <= 0.85:
                return 0.03 - (fr - 0.72)*0.02/0.13
            else:
                return 0
        df_b = df[df['wdl']<0.78]
        df_b['type'] = 'B'
        df_bc = df[~(df['wdl']<0.78)]
        df_bc['type'] = 'BC'
        df_bc['budget'] = df_bc['gmv_fence']*(df_bc['wdl'].apply(fr_to_sr))
        df_new = df_b.append(df_bc)
        return df_new

    
    def get_pitfill_budget(self, data):
        def get_tsh_ratio_gap(line):
            fcnt = line['fcnt']
            wdl = line['wdl']
            fr_ratio = line['ideal_fr_rate']
            model_params = line['fr_model_params']
            supply_0 = np.log(1 - wdl)*fcnt/model_params
            supply_1 = np.log(1 - fr_ratio)*fcnt/model_params
            delta_supply = max(supply_1-supply_0,0)
            tsh_ratio_gap = delta_supply/supply_0
            return tsh_ratio_gap 

        def tsh_ratio_to_budget_ratio(line):
            dic = line['candidate_dic']
            gap = line['tsh_ratio_gap']
            if gap == 0:
                return 0
            for b in dic:
                if dic[b] > gap:
                    return b
            return b
        
            
        def get_ideal_fr_rate(city_id):
            thres = pd.read_csv('./config/thres.txt', sep = '\t')
            return thres[thres['city_id'] == city_id]['finish_rate'].values[0]
            
        fr_model_param_dict = self.get_fr_model_params()
        data['ideal_fr_rate'] = data['city_id'].apply(get_ideal_fr_rate)
        data['fr_model_params'] = data['city_id'].apply(lambda c: fr_model_param_dict[c]['sd_coef'])
        data['tsh_ratio_gap'] = data.apply(get_tsh_ratio_gap, axis = 1)
        data['budget_ratio'] = data.apply(tsh_ratio_to_budget_ratio, axis = 1)
        data['budget'] = data['budget_ratio']*data['gmv_fence']
        return data
    
    def get_final_budget(self, data):
        data = self.get_pitfill_budget(data)
        data_new = self.fill_budget_0(data)
        print('summary:B and BC')
        print(data_new[['type', 'dt', 'budget']].groupby('type').sum().reset_index())
        print('summary:num of activity with 0 gmv ')
        print(data_new[~(data_new['gmv_fence']>0)].shape)
        print('summary:num of activitys')
        print(data_new.shape)
        self.upload_pitfill_budget(data_new)
        print('compute budget over')
        return data_new
        
        