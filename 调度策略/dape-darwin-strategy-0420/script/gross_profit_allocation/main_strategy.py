#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from ctypes import util
from random import gammavariate
import sys
import logging
import traceback
import requests
import utils
from pyspark import SparkConf, SparkContext
from pyspark import SparkFiles
from pyspark.sql import HiveContext
from pyspark.sql.functions import col, udf, date_sub, explode, split, date_add
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import time
import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace
from pyspark.sql import functions as sf
from ortools.sat.python import cp_model
from ortools.linear_solver import pywraplp
import json
from utils import *
from hive_sql import *
from functools import reduce
import subprocess
import warnings

import datetime
warnings.filterwarnings("ignore")
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.set_option('expand_frame_repr', False)
logger = logging.getLogger(__name__)
CANDIDATE_LEN = 7
MONEY_UNIT = 100.0  # 分
num_of_expand_days = 14  # 扩充20天数据
MIN_NUM_OF_DATA = 0
DIAODU_SCALE = 0.0028353391525316315
GAP_RATE_DEFAULT = 0.9
DIAODU_SUBSIDY_UPBOUND= 30000.0

class Strategy(object):
    def __init__(self, config, stg_param, mode):
        self._config = config
        self._stg_param = stg_param
        self._df = None
        conf = SparkConf().setAppName('bc_realloc_system')
        self.sc = SparkContext(conf=conf)
        self.hc = HiveContext(self.sc)
        self._config['bc_table_path'] = self._config["online_request_bc_table_path"]  # gallin test
        if (mode == 2):
            self._config['bc_table_path'] = self._config["online_request_bc_table_path"]
        # 接口调用
        if stg_param['is_online'] == "online":
            self._api_url = config['callback_api']
        elif stg_param['is_online'] == "pre":
            self._api_url = config['callback_api_pre']
        elif stg_param['is_online'] == "test":
            self._api_url = config['callback_api_test']
    

    def return_b_gap_info(self, choose_scheme,forecast_daily_data, asp_df):

        res_df = pd.DataFrame()
        for city in set(choose_scheme['city_id']):
            city_df = choose_scheme.query("city_id == %d"%(city))
            for day in set(city_df['stat_date']):
                temp = {}
                city_day_df = city_df.query("stat_date == '%s'"% (day))
                ori_b_budget = 0.0
                ori_acc_budget = 0.0
                delta_rides = 0.0
                gmv = 0.0
                pangu_stg_detail = []
                acc_card_stg_detail = []
                for i in range(len(city_day_df)):
                    if city_day_df['tool'].iloc[i] == '智能盘古':
                        ori_b_budget = ori_b_budget + city_day_df['subsidy'].iloc[i]
                    else:
                        ori_acc_budget = ori_acc_budget + city_day_df['subsidy'].iloc[i]
                    if city_day_df['pangu_stg_detail'].iloc[i] != "" :
                        pangu_stg_detail.append(city_day_df['pangu_stg_detail'].iloc[i])
                    if city_day_df['acc_card_stg_detail'].iloc[i] != "":
                        acc_card_stg_detail.append(city_day_df['acc_card_stg_detail'].iloc[i])

                    delta_rides = delta_rides + city_day_df['delta_ride'].iloc[i]
                
                if city in set(asp_df['city_id']):
                    asp = asp_df.query("city_id == '%d'"%(city))['asp'].values[0]
                elif forecast_daily_data.query("stat_date == '%s' & city_id == '%d'"%(day,city)).shape[0] != 0:
                    asp = forecast_daily_data.query("stat_date == '%s' & city_id == '%d'"%(day,city))['asp'].values[0]
                else:
                    self.error_callback('b端周分配 获取asp数据失败！')
                gmv = forecast_daily_data.query("stat_date == '%s' & city_id == '%d'"%(day,city))['total_gmv'].values[0]

                temp['city_id'] = city
                temp['pred_date'] = day
                temp['b_budget'] = round(ori_b_budget,4)
                temp['acc_budget'] = round(ori_acc_budget,4)
                temp['pangu_stg_detail'] = pangu_stg_detail
                temp['acc_card_stg_detail'] = acc_card_stg_detail
                temp['delta_ride'] = round(delta_rides,4)
                temp['gmv'] = round(gmv,4)
                temp['asp'] = round(asp,4)
                temp['delta_gmv'] = round(delta_rides*asp,4)



                res_df = res_df.append(pd.DataFrame([temp]))
        return res_df

    def agg_event_2_city_options(self,x):
        '''
        聚合到城市维度，并将活动信息转换成dict存储在新的字段里
        :return:
        '''
        pangu_stg_detail = ""
        acc_card_stg_detail = ""
        event_dict = {
            "fence_id": x.fence_id,
            "start_time": x.start_time,
            "end_time": x.end_time,
            "amount":round(x.subsidy,4),
            "interval_cr":round(x.cr,4),
            "pit_depth":round(x.cr_bar-x.cr,4),
            "interval_gmv": round(x.gmv,4)

        }
        if x.tool == "智能盘古":
            pangu_stg_detail = event_dict
        elif x.tool == "加速卡" :
            acc_card_stg_detail = event_dict

        return pangu_stg_detail, acc_card_stg_detail

    def gap_find_best_option(self, gap_pred_info,acc_budget):
        # 增加系数 CON 来保证异常率最小的前提下花费最小,一个任意足够大的数
        CON = 10000
        x = {}
        solver = pywraplp.Solver.CreateSolver('CBC')
        solver.max_time_in_seconds = 240.0
        for i in range(len(gap_pred_info)):
            x[i] = solver.NumVar(0.0, 1, 'x[%i]' % (i))

        pangu_remain_budget = self.params["b_total_budget"] - acc_budget
        print("********留给盘古填坑的预算：**********",pangu_remain_budget)

        solver.Add(solver.Sum([x[i] * gap_pred_info['subsidy'].iloc[i]
                               for i in range(0, len(x))]) <= self.params["b_total_budget"] - acc_budget)

        solver.Add(solver.Sum([x[i] * gap_pred_info['subsidy'].iloc[i]
                               for i in range(0, len(x))]) >= 0)


        for group_idx in list(set(gap_pred_info['group_idx'])):
            start_time = group_idx.split('_')[0]
            city = int(group_idx.split('_')[1])
            fence_id = int(group_idx.split('_')[2])
            stat_date = group_idx.split('_')[3]
            eventdf = gap_pred_info.query("city_id == %d & fence_id == %d & stat_date == '%s' & start_time == '%s'  " % (city,fence_id, stat_date,start_time))
            idxlist = []
            if len(eventdf) >1 :
                for j in range(0, len(eventdf)):
                    idxlist.append(eventdf.iloc[j]['idx'])
                solver.Add(solver.Sum([x[idxlist[i]] for i in range(0, len(idxlist))]) == 1)

        print('Number of variables =', solver.NumVariables())
        print('Number of constraints =', solver.NumConstraints())
        # solver.Minimize(solver.Sum([x[i] * gap_pred_info['abn_rate'].iloc[i] for i in range(0, len(x))]))
        # 求解目标变为异常率最小的前提下补贴金额也尽可能
        solver.Minimize(solver.Sum([x[i] * gap_pred_info['abn_rate'].iloc[i] * CON + x[i] * gap_pred_info['subsidy'].iloc[i] for i in range(0, len(x))]))
        status = solver.Solve()

        reslist = []
        if status == pywraplp.Solver.OPTIMAL:
            print('Solution:')
            print('Objective value =', solver.Objective().Value())

            tempdict = {}
            for i in range(0, len(x)):
                cur_val = x[i].solution_value()
                if cur_val > 0.0:
                    tempkey = '%s_%s' % (gap_pred_info['group_idx'].iloc[i], gap_pred_info['stat_date'].iloc[i])
                    if not tempkey in tempdict:
                        tempdict[tempkey] = (cur_val,i)
                    else:
                        his_max_val,his_idx = tempdict[tempkey]
                        if cur_val > his_max_val:
                            tempdict[tempkey] =  (cur_val,i)
            for (k,v) in tempdict.items():
                reslist.append(v[1])
            gap_choose_scheme = gap_pred_info.iloc[reslist, :]
        print("B端填坑阶段总花费：",gap_choose_scheme['subsidy'].sum())
        print("B端填坑总预算：",pangu_remain_budget )
        return gap_choose_scheme

    def get_api_pred_info(self):
        pred_api_url  = self._api_url + "queryengine/predictvalue"
        query_info_dict = {
            "day_predict:gmv":[110100,110000,110103,'pukuai'], # 快车，网约车，特惠, 普快
            "day_predict:total_tsh":[110100,110000,110103],
            "day_predict:finish_order_cnt":[110100,110000,110103],
            "day_predict:compete_call_cnt":[110100,110000,110103],
            "day_predict:objective_call_cnt":[110100,110000,110103]
        }
        api_pred_info = pd.DataFrame()
        for query_key in query_info_dict: 
            query_msg = {
                "dt": self.params["gmv_dt"],
                "data_type": query_key,
                "st_date_list": self.params["st_end_times"] ,
                "city_list":self.params["city_list_l"],
                'pc_list':query_info_dict[query_key] 
            }
            resp = callback_request(pred_api_url,query_msg )
            sub_pred_info = pd.DataFrame(resp['data']['data'])
            api_pred_info  = pd.concat([api_pred_info,sub_pred_info], axis = 0)
        print("api_pred_info shape", api_pred_info.shape[0])
        print(api_pred_info.head())
        self.hc.createDataFrame(api_pred_info).registerTempTable('api_pred_info')
        # self.hc.createDataFrame(api_pred_info).write.mode('overwrite').saveAsTable('bigdata_driver_ecosys_test.temp_api_pred_info')
        return api_pred_info


    def get_raw_gf_data(self):
        # 获取和毛利相关的字段
        # 1、gmv 数据获取 2、tr 数据获取 3、ykj获取 
        api_pred_info = self.get_api_pred_info() 
        print(pred_and_tr_ykj_info_sql.format(**self.params))
        pred_and_tr_ykj_info_pd = self.hc.sql(pred_and_tr_ykj_info_sql.format(**self.params)).toPandas()

        # 数据空校验
        if pred_and_tr_ykj_info_pd.shape[0] == MIN_NUM_OF_DATA:
            self.error_callback('C端T+1分配-获取TR和一口价数据失败 !')
        else:
            pred_and_tr_ykj_info_pd['gmv_pk'] = pred_and_tr_ykj_info_pd.apply(lambda x: x.gmv_pk if x.gmv_pk < x.gmv_k else x.gmv_k,axis = 1 )
            pred_and_tr_ykj_info_pd['tr_th_amount'] = pred_and_tr_ykj_info_pd['take_rate_th'] * pred_and_tr_ykj_info_pd['gmv_th']
            pred_and_tr_ykj_info_pd['tr_k_amount'] = pred_and_tr_ykj_info_pd['take_rate_k'] * pred_and_tr_ykj_info_pd['gmv_k']
            pred_and_tr_ykj_info_pd['ykj_amount'] = pred_and_tr_ykj_info_pd['ykj_th'] * pred_and_tr_ykj_info_pd['gmv_th']
            print("successfully get the tr and ykj info ,as follows", pred_and_tr_ykj_info_pd.head())
            self.hc.createDataFrame(pred_and_tr_ykj_info_pd).registerTempTable('pred_and_tr_ykj_info')
            pred_and_tr_ykj_info_sp = self.hc.createDataFrame(pred_and_tr_ykj_info_pd)
            pred_and_tr_ykj_info_sp.repartition(10).write.mode('overwrite').csv("pred_gmv_step_start_date_%s_end_date_%s"
                                    % (self.params['step_start_date'], self.params['end_time']), header=True)
        return pred_and_tr_ykj_info_pd['tr_th_amount'].sum(), pred_and_tr_ykj_info_pd['tr_k_amount'].sum(), pred_and_tr_ykj_info_pd['ykj_amount'].sum()


    def label_search_space_udf(self,json_dict):
        '''
        pyspark udf实现，对在Apollo中的补贴率进行筛选
        :param json_acc_dict:
        :return:
        '''
        # def get_round_b_rate(b_rate):
        #     b_rate1,b_rate2,b_rate3,b_rate4 = round(b_rate-0.002,3),round(b_rate-0.004,3),round(b_rate+0.002,3),round(b_rate+0.004,3)
        #     return b_rate,b_rate1,b_rate2,b_rate3,b_rate4

        def label_search_space( city_id, b_rate,tool):
            rate_dict = json.loads(json_dict)
            label = 0
            if str(city_id) in rate_dict.keys():
                if tool == '加速卡':
                    label = 1
                else:
                    for br in rate_dict[str(city_id)]:
                        # b_rate_list  = get_round_b_rate(br) 
                        if (b_rate <= round(br + 0.005, 3) and b_rate>= round(br - 0.005, 3)) or b_rate == 0:
                            label = 1
            # 若该城市没有在apollo配置，则用默认city_id 0(战区城市可用)
            else:
                if tool == '加速卡':
                    label = 1
                else:
                    for br in rate_dict[str(0)]:
                        # b_rate_list  = get_round_b_rate(br) 
                        if (b_rate <= round(br + 0.005, 3) and b_rate>= round(br - 0.005, 3)) or b_rate == 0:
                            label = 1
            return label
            
        return F.udf(label_search_space, IntegerType())

    def get_hourly_indicator(self, x):
        '''
        返回对应补贴率下的该活动时段内的gmv，以及呼叫数
        '''
        ROI_FACTOR = 1.0  # 用来放缩在线时长对完单率的影响
        if int(x.stat_hour) < int(x.start_time[0:2]) or (int(x.stat_hour)>= int(x.end_time[0:2]) and x.end_time[3:5] != '30') or (int(x.stat_hour) > int(x.end_time[0:2])):
            gmv_h = 0.0
            call_order_cnt_h = 0.0
            pred_cr = x.cr
        elif (int(x.stat_hour) == int(x.start_time[0:2]) and  x.start_time[3:5] == '30') or (int(x.stat_hour) == int(x.end_time[0:2]) and x.end_time[3:5] == '30'):
            gmv_h = 0.5 * x.gmv_ratio * x.total_gmv  # 区县活动小时内的gmv，用来计算budget
            call_order_cnt_h = 0.5 * x.call_count_ratio * x.call_order_cnt # 区县活动内小时的call，后续用来计算供需
            pred_cr = x.cr * (1 + x.delta_tsh * ROI_FACTOR) # 如果补贴率为0，对应的pred_cr计算值应该是cr
        else:
            gmv_h = x.gmv_ratio * x.total_gmv
            call_order_cnt_h = x.call_count_ratio * x.call_order_cnt
            pred_cr = x.cr * (1+x.delta_tsh * ROI_FACTOR)
        
        delta_cr_h = pred_cr - x.cr  # delta cr
        delta_rides_h = delta_cr_h * call_order_cnt_h
        cr_gap_h = x.cr_bar - pred_cr # cr_gap aft sub
        # pred_cr_h = x.cr_bar if pred_cr > x.cr_bar else pred_cr   #FIXME 后续步长较大的情形
        pred_cr_h = pred_cr  
        abn_rate_h = call_order_cnt_h * (pred_cr_h - x.cr_bar)**2  #成交异常率 
        subsidy_h = gmv_h * x.b_rate

        return gmv_h ,call_order_cnt_h, delta_cr_h, delta_rides_h, abn_rate_h, subsidy_h, cr_gap_h


    def find_opti_c_choose(self,opti_df, c_budget):
        opti_data = opti_df[opti_df["pred_date"] >= self.params['step_start_date']]
        print("opti_data length for lp:", len(opti_data))
        opti_data['idx'] = list(range(opti_data.shape[0]))
        
        start_a = datetime.datetime.now()

        x = {}
        solver = pywraplp.Solver.CreateSolver('CBC')
        solver.max_time_in_seconds = 240.0
        for i in range(len(opti_data)):
            x[i] = solver.NumVar(0.0, 1, 'x[%i]' % (i))

        # 添加条件约束
        solver.Add(solver.Sum([x[i] * opti_data['budget_pk'].iloc[i]+  x[i] * opti_data['budget_th'].iloc[i] 
                            for i in range(0, len(x))]) <= c_budget)
        solver.Add(solver.Sum([x[i] * opti_data['budget_pk'].iloc[i]
                for i in range(0, len(x))]) >= 0)
        solver.Add(solver.Sum([x[i] * opti_data['budget_th'].iloc[i]
                for i in range(0, len(x))]) >= 0)

        # 添加参数约束
        for city in list(set(opti_data['city_id'])):
            for day in pd.date_range(self.params['step_start_date'], self.params['end_time']).astype(str):
                citydf = opti_data.query("city_id == %d  & pred_date == '%s'" % (city, day))
                #print("day and city_id ",day ,city)
                idxlist = []
                if len(citydf) > 0 :
                    #print("len(citydf) and city_id ",len(citydf))
                    for j in range(0, len(citydf)):
                        idxlist.append(citydf.iloc[j]['idx'])
                    solver.Add(solver.Sum([x[idxlist[i]] for i in range(0, len(idxlist))]) == 1)

        print('Number of variables =', solver.NumVariables())
        print('Number of constraints =', solver.NumConstraints())
        solver.Maximize(solver.Sum([x[i] * opti_data['d_gmv_k'].iloc[i] + x[i] * opti_data['d_gmv_th'].iloc[i]
                    for i in range(0, len(x))]))
        status = solver.Solve()

        reslist = []
        start_b = datetime.datetime.now()
        print("规划总计花费时长(s): ",(start_b - start_a).seconds)
        if status == pywraplp.Solver.OPTIMAL:
            print('Solution:')
            print('Objective value =', solver.Objective().Value())

            tempdict = {}
            for i in range(0, len(x)):
                cur_val = x[i].solution_value()
                if cur_val > 0.0:
                    tempkey = '%d_%s' % (opti_data['city_id'].iloc[i], opti_data['pred_date'].iloc[i])
                    # print("cur_val is {} ,tempkey is {}".format(cur_val,tempkey))
                    if not tempkey in tempdict:
                        tempdict[tempkey] = (cur_val,i)
                    else:    
                        his_max_val,his_idx = tempdict[tempkey]
                        if cur_val > his_max_val:
                            tempdict[tempkey] =  (cur_val,i)
            for (k,v) in tempdict.items():
                reslist.append(v[1])
            choose_scheme = opti_data.iloc[reslist, :]
            print(choose_scheme.groupby(['pred_date']).count())

            cur_costs = choose_scheme['budget_pk'].sum() + choose_scheme['budget_th'].sum()
            gap = c_budget - cur_costs
            print('总预算是 {} ,已规划预算是{}, 普快预算{}, 特惠预算{} , 规划和预算的gap是{} :'.format(c_budget, cur_costs,choose_scheme['budget_pk'].sum(), choose_scheme['budget_th'].sum(), gap))
            # if gap >= 0.3 * c_budget:
            #     self.error_callback("呼返T+1-预算剩余比例超过30% ！!")
            if(gap != 0):
                cur_gap_costs = choose_scheme['budget_pk'].sum() + choose_scheme['budget_th'].sum()
                choose_scheme['budget_pk'] = choose_scheme['budget_pk'].apply(lambda x: x + x * gap / cur_gap_costs)
                choose_scheme['budget_th'] = choose_scheme['budget_th'].apply(lambda x: x + x * gap / cur_gap_costs)
                choose_scheme['treat_subsidy_hufan_rate_th'] = choose_scheme['budget_th'] / choose_scheme['gmv_th']
                choose_scheme['treat_subsidy_hufan_rate_pk'] = choose_scheme['budget_pk'] / choose_scheme['gmv_pk']
        else:
            print('The problem does not have an optimal solution.')
            choose_scheme = opti_data.query('(budget_pk == 0.0) and (budget_th == 0.0)')
        return choose_scheme


    def get_opti_data(self, c_elestic_pred_info, max_try_num = 7):
        """
        通过表join 获取完单率模型参数
        计算对应的dgmv 
        """
        for try_num in range(max_try_num):
            self.params['gongxu_try_num'] = try_num
            print(gongxu_info_sql.format(**self.params))
            gongxu_info_pd = self.hc.sql(gongxu_info_sql.format(**self.params)).toPandas()
            print("gongxu shape",gongxu_info_pd.shape )
            print(gongxu_info_pd.head())
            if gongxu_info_pd.shape[0] != MIN_NUM_OF_DATA :
                break
            if(self.params['gongxu_try_num'] == max_try_num-1) :
                self.error_callback('C端T+1分配-获取完单预估数据失败 !')
        
        # merge and get dgmv 
        print(gongxu_info_pd.isnull().any())
        gongxu_info_pd = gongxu_info_pd.dropna() # 需要检查为什么会有空

        opti_df = pd.merge(c_elestic_pred_info, gongxu_info_pd, on = ['city_id', 'pred_date','dsr_k_cpt','dsr_th_cpt'], how = 'inner')
        opti_df['budget_k'] = opti_df['treat_subsidy_hufan_rate_pk'] * opti_df['gmv_k']
        opti_df['budget_pk'] = opti_df['treat_subsidy_hufan_rate_pk'] * opti_df['gmv_pk'] 
        opti_df['budget_th'] = opti_df['treat_subsidy_hufan_rate_th'] * opti_df['gmv_th']  
        opti_df['d_gmv_k'] = opti_df['call_k_cpt'] * opti_df['pred_dsend_ratio_k'] * opti_df['pred_cr_k'] * opti_df['asp_k']
        opti_df['d_gmv_th'] = opti_df['call_th_cpt'] * opti_df['pred_dsend_ratio_th'] * opti_df['pred_cr_th'] * opti_df['asp_th']

        opti_sp = self.hc.createDataFrame(opti_df)
        opti_sp.repartition(10).write.mode('overwrite').csv("opti_df_step_start_date_%s_end_date_%s"
                        % (self.params['step_start_date'], self.params['end_time']), header=True)
        return opti_df

    def get_c_elasticity(self, max_try_num = 15):
        """
        获取C端呼返弹性数据
        如果没有最新分区的数据，采取往前采用兜底的方案，否则报 error
        """
        for try_num in range(max_try_num):
            self.params['c_elestic_try_num'] = try_num
            print(c_elestic_pred_info_sql.format(**self.params))
            c_elestic_pred_info = self.hc.sql(c_elestic_pred_info_sql.format(**self.params)).toPandas()
            c_elestic_pred_info['dsr_k_cpt'] = c_elestic_pred_info.apply(lambda x: 0.8 if x.dsr_k_cpt > 0.8 else x.dsr_k_cpt, axis=1)
            c_elestic_pred_info['dsr_th_cpt'] = c_elestic_pred_info.apply(lambda x: 0.8 if x.dsr_th_cpt > 0.8 else x.dsr_th_cpt, axis=1)

            if c_elestic_pred_info.shape[0] != MIN_NUM_OF_DATA :
                self.hc.createDataFrame(c_elestic_pred_info).repartition(1).registerTempTable('c_elestic_pred_info')
                c_elestic_pred_info_df = self.hc.createDataFrame(c_elestic_pred_info)
                # c_elestic_pred_info_df.write.mode('overwrite').saveAsTable('bigdata_driver_ecosys_test.temp_c_elestic_pred_info_df')
                break
            if(self.params['c_elestic_try_num'] == max_try_num-1) :
                self.error_callback('C端T+1分配-获取弹性数据失败 !')

        return c_elestic_pred_info

    def get_c_elasticity_ori(self, max_backup_num=7):
        # c端弹性，backup方案，往前找
        st_end_times = sorted(self.params['st_end_times'])
        start_time, end_time = st_end_times[0], st_end_times[-1]
        for try_num in range(max_backup_num):
            self.params['c_elasticity_try_num'] = try_num
            print('c elasticity try num :', try_num)
            print(c_elasticity_sql.format(**self.params))
            c_delta_send_rate = self.hc.sql(c_elasticity_sql.format(**self.params))
            if(c_delta_send_rate.count() == MIN_NUM_OF_DATA):
                continue
            max_date = max(c_delta_send_rate.toPandas()['pred_date'])
            max_date_df = c_delta_send_rate.filter(
                'pred_date == "{}"'.format(max_date))

            c_delta_send_rate_dfs = [c_delta_send_rate]
            for i in range(1, num_of_expand_days, 1):
                df_elem = max_date_df.withColumn('pred_date', date_add(col('pred_date'), i))\
                                     .withColumn('pred_date', col('pred_date').astype("string"))
                c_delta_send_rate_dfs.append(df_elem)
            month_level_c_delta_send_rate_df = reduce(DataFrame.unionAll, c_delta_send_rate_dfs)
            month_level_c_delta_send_rate_df = month_level_c_delta_send_rate_df.filter(
                                                "pred_date in ({st_end_times_str}) ".format(**self.params))

            pred_start_num = month_level_c_delta_send_rate_df.filter('pred_date == "{}"'.format(start_time)).count()
            pred_end_num = month_level_c_delta_send_rate_df.filter('pred_date == "{}"'.format(end_time)).count()
            print(pred_start_num, pred_end_num)
            if(pred_start_num > MIN_NUM_OF_DATA and pred_end_num > MIN_NUM_OF_DATA):
                print('c elasticity backup num: {},{}'.format(pred_start_num, pred_end_num))
                month_level_c_delta_send_rate_df.registerTempTable("month_level_c_delta_send_rate_df")
                return month_level_c_delta_send_rate_df
        self.error_callback('调度分配-获取C端弹性数据失败!')

    def get_elastic_b_hourly(self, df_sample, max_try_num = 7):
        """
        1、获取B端弹性和供需数据结合数据
        2、弹性数据一定会有，允许选择之前分区
        3、如果兜底仍然没有，需要报错人工介入
        """
        json_pangu_rate_dict = json.dumps(self.params["pangu_rate_dict"])
        json_accelerate_card_rate_dict = json.dumps(self.params["accelerate_card_rate_dict"])

        city_list = [int(city_id) for city_id in self.params['city_list'].split(',')]
        stat_date_list = list(df_sample['stat_date'].unique())
        stat_hour_list = [int(i) for i in np.linspace(6,23,18)]
        b_rate_list = [round(i,3) for i in np.linspace(0.0,0.15,151)]
        multi_index = pd.MultiIndex.from_product([city_list,stat_date_list,stat_hour_list,b_rate_list], names=['city_id', 'start_dt','start_time','b_rate'])
        base_ela_df = pd.DataFrame(index=multi_index).reset_index()
        base_ela_df['estimate'] = base_ela_df['b_rate'] * 1.2  # 弹性
        base_ela_py_df = self.hc.createDataFrame(base_ela_df)
        base_ela_py_df.registerTempTable('fake_b_ela_table')

        

        print("SQL of pred_and_pangu_elastic_sql:",pred_and_pangu_elastic_sql.format(**self.params))
        pangu_df = self.hc.sql(pred_and_pangu_elastic_sql.format(**self.params))
        pangu_elastic_df = pangu_df.withColumn('label',self.label_search_space_udf(json_pangu_rate_dict) \
                                        (pangu_df.city_id, pangu_df.b_rate, pangu_df.tool).cast('int')).filter("label == 1").drop("label")

        print("SQL of pred_and_acc_elastic_sql:",pred_and_acc_elastic_sql.format(**self.params))
        acc_df = self.hc.sql(pred_and_acc_elastic_sql.format(**self.params))
        acc_elastic_df = acc_df.withColumn('label',self.label_search_space_udf(json_accelerate_card_rate_dict) \
                                            ( acc_df.city_id, acc_df.b_rate, acc_df.tool).cast('int')).filter("label == 1").drop("label")

        pangu_elastic = pangu_elastic_df.toPandas() 
        #pangu_elastic['delta_tsh'] = pangu_elastic['b_rate'] * 1.2    盘古1.2弹性已经在上面fake表中计算
        print("盘古弹性天分布：",pangu_elastic.groupby('stat_date').count())

        acc_elastic = acc_elastic_df.toPandas()
        acc_elastic['b_rate'] = 0.04 # sql中也写0.04，加速卡调整可以不修改sql 直接在这里临时调整
        acc_elastic['delta_tsh'] = acc_elastic['b_rate'] * 1.2
        print("加速卡弹性天分布：",acc_elastic.groupby('stat_date').count())

        return pangu_elastic, acc_elastic       


    def get_pregap_dignosis_info(self, df_sample):

        '''
        1、获取供需诊断数据，兜底选择分区
        2、供需诊断数据在屏蔽前不为空判断 
        TODO 实现多个分区数据获取
        '''    

        version_list = json.loads(self.params['version_list']) 
        # 根据不同版本获取诊断数据对应的分区 
        if self.params['cr_type'] == 'objective_exp_openapi_pp':
            fixed_threshold_list = [('fixed_threshold_'+str(version['cr'])[2:]) for version in version_list]
            allocate_version_list = [('客观-'+ version['version_code'].upper()) for version in version_list]
        else:
            fixed_threshold_list = [('exp_fixed_threshold_'+str(version['cr'])[2:]) for version in version_list]
            allocate_version_list = [('体验-'+version['version_code'].upper()) for version in version_list]
        fixed_threshold_str = str(fixed_threshold_list)[1:-1]
        self.params['fixed_threshold_str'] = fixed_threshold_str

        print("SQL of gap_dignosis: ",gap_dignosis_sql.format(**self.params))
        gap_dignosis_pre = self.hc.sql(gap_dignosis_sql.format(**self.params)).toPandas()
        # 首先判断分区是否有数据，没有进行分区兜底
        if gap_dignosis_pre.shape[0] == MIN_NUM_OF_DATA :
            self.error_callback('B端周分配-获取诊断信息失败 !')
        # 判断已经获取的诊断数据分区是否满足输入要求
        if len(version_list) != len(list(gap_dignosis_pre['strategy_type'].unique())):
            self.error_callback("the kinds of gap dignosis strategy_type is less than version list")
    
        gap_dignosis = gap_dignosis_pre.merge(df_sample,on=['stat_date','city_id','tool'], how = 'inner')
        # 过滤当前cr已经大于cr bar的数据，不需要进入诊断数据
        gap_dignosis = gap_dignosis.query('cr<cr_bar')
        gap_dignosis['idx_temp'] = ['_'.join(i) for i in gap_dignosis[['city_id','stat_date','tool']].values.astype(str)] # 添加唯一idx ，作为该区县时段唯一标识
        
        gap_dignosis_all_strategy = gap_dignosis
        if gap_dignosis_all_strategy.shape[0] == MIN_NUM_OF_DATA:
            print("None of city will allocate with B ")
            gap_dignosis_all_strategy = pd.DataFrame(columns=['city_id', 'stat_date', 'fence_id', 'tool', 'strategy_type','start_time', 'start_hour',
            'end_time', 'end_hour', 'cr', 'cr_bar', 'gmv_ratio','finish_count_ratio', 'call_count_ratio'])
        else:      
            gap_dignosis_all_strategy = gap_dignosis_all_strategy[['city_id', 'stat_date', 'fence_id', 'tool', 'strategy_type','start_time', 'start_hour',
            'end_time', 'end_hour', 'cr', 'cr_bar', 'gmv_ratio','finish_count_ratio', 'call_count_ratio']]
            print("供需诊断分布",gap_dignosis_all_strategy.groupby(['stat_date','strategy_type']).count())

        return gap_dignosis_all_strategy ,fixed_threshold_list , allocate_version_list

    
    def get_asp_df(self):
        #获取城市asp数据，过去一周的asp均值作为城市asp        
        print("SQL of get_asp_df: ",asp_sql.format(**self.params))
        asp_df_all =  self.hc.sql(asp_sql.format(**self.params)).toPandas().astype({'asp':'float'})
        if(asp_df_all.shape[0] == 0):
            self.error_callback('B端周分配-获取asp数据失败!')
        return asp_df_all


    def get_roi_df(self, max_backup_num=7):
        # 后验roi，backup方案，往前找
        for try_num in range(max_backup_num):
            self.params['roi_try_num'] = try_num
            print('roi try num :', try_num)
            print(posroi_sql.format(**self.params))
            posroi_info = self.hc.sql(posroi_sql.format(**self.params)).toPandas()
            cis = list(map(int, self.params['city_list'].split(',')))+[-1]
            posroi_info = posroi_info.query("city_id in @cis")
            print("city number:", len(posroi_info.city_id.unique()))
            if len(posroi_info) > 0:
                if len(posroi_info.city_id.unique()) < 0.5 * len(cis):
                    print("分配城市列表中，后验roidf至少缺失一半城市！！")
                    continue
                ctl_roidf = posroi_info.query("group=='ctl'")
                exp_roidf = posroi_info.query("group=='exp'")
                if len(ctl_roidf) == 0 or len(exp_roidf) == 0:
                    continue
                cols = ['pkhf', 'pkroi_c', 'pkroi_bp', 'thhf', 'throi_c', 'throi_bp', 'fkroi_c', 'fkroi_bp', 'fkhf']
                dd = dict(zip(cols, ['our_' + col for col in cols]))
                exp_roidf.rename(columns=dd, inplace=True)
                roidf = pd.merge(ctl_roidf, exp_roidf, on=['city_id'], how='left')
                roidf.replace({np.nan: 0, np.inf: 0, -np.inf: 0}, inplace=True)
                print("roidf length:", len(roidf))
                return roidf
        if (self.params['roi_try_num'] == max_backup_num - 1):
            self.error_callback('C端T+1分配：获取历史roi信息失败!')
    
    def get_ctl_estimate_rate(self, c_budget, opti_df, max_backup_num=7):
        for try_num in range(max_backup_num):
            self.params['rate_try_num'] = try_num
            print('rate try num :', try_num)
            print(ctlrate_sql.format(**self.params))
            rate_info = self.hc.sql(ctlrate_sql.format(**self.params)).toPandas()

            cis = list(map(int, self.params['city_list'].split(',')))+[-1]
            rate_info = rate_info.query("city_id in @cis")
            print("city number:", len(rate_info.city_id.unique()))
            if len(rate_info) > 0:
                if len(rate_info.city_id.unique()) < 0.5 * len(cis):
                    print("分配城市列表中，对照组补贴率数据至少缺失一半城市！！")
                    continue
                alloc_opti_df = opti_df[['city_id', 'pred_date']].drop_duplicates()
                rate_info = rate_info.query("pred_date >= '%s' and pred_date <= '%s'" % (self.params['step_start_date'], self.params['end_time']))
                alloc_rate_info = pd.merge(alloc_opti_df, rate_info, on=['city_id', 'pred_date'], how='inner')
                alloc_rate_info.fillna(0, inplace=True)
                print('alloc rate info:', alloc_rate_info['pred_date'].unique())
                history_budget = alloc_rate_info['budget_th'].sum() + alloc_rate_info['budget_pk'].sum()
                print("历史估计预算：", history_budget)
                if history_budget == 0:
                    self.error_callback('C端T+1分配：获取历史预算为0!')
                ratio = c_budget / history_budget
                print("ctrl补贴率缩放比例:", ratio)

                tehuidf = rate_info.query("pred_date >= '%s' and pred_date <= '%s'" % (self.params['step_start_date'], self.params['end_time']))[['city_id', 'pred_date', 'thhf_past']]
                kuaidf = rate_info.query("pred_date >= '%s' and pred_date <= '%s'" % (self.params['step_start_date'], self.params['end_time']))[['city_id', 'pred_date', 'pkhf_past']]

                tehuidf['thhf_past'] = tehuidf['thhf_past'] * ratio
                kuaidf['pkhf_past'] = kuaidf['pkhf_past'] * ratio
                ctl_kuaidf = kuaidf.rename(columns={'pkhf_past': 'ctl_pkhf'})
                ctl_tehuidf = tehuidf.rename(columns={'thhf_past': 'ctl_thhf'})
                return ctl_kuaidf, ctl_tehuidf
        if (self.params['rate_try_num'] == max_backup_num - 1):
            self.error_callback('C端T+1分配：获取历史补贴率信息失败!')


    def pid_filter_opti_data(self, opti_data, roidf, ctl_kuaidf, ctl_tehuidf, hexiao_ratedict, topcitystr):

        nationdf = roidf.query("city_id==-1") 
        if len(nationdf) > 0:
            # 【全国throi，全国th毛利roi，全国fk roi，全国fk毛利roi】
            na_throi_c, na_throi_bp, na_fkroi_c, na_fkroi_bp = nationdf[['throi_c', 'throi_bp', 'fkroi_c', 'fkroi_bp']].iloc[0]
            print("nathroi_c, na_throi_bp, na_fkroi_bp, na_fkroi_c：",na_throi_c, na_throi_bp, na_fkroi_bp, na_fkroi_c)
        else:
            self.error_callback('C端T+1分配：全量城市的后验roi缺失!')

        # 加特惠时，补贴率的放大倍数
        large_base, middle_base, small_base = 1.8, 1.5, 1.2
        # 加特惠的补贴率绝对值
        large_abs, middle_abs, small_abs = 0.09, 0.06, 0.03  # 9pp, 6pp, 3pp

        throi_bp_large_coef = 0.6  # 大幅加特惠时，throi_bp >=0.6*均值
        throi_bp_middle_coef = 0.3  # 中幅加特惠时，throi_bp < 0.6*均值 & throi_bp > max(0.3*特惠均值, 泛快均值)
        throi_bp_small_coef = 0.2  # 小幅加特惠时，throi_bp < 0.3*均值 & throi_bp > max(0.2*特惠均值, 泛快均值)

        # 判断泛快roi好的充分必要条件：fkroi_c >= 1.2 
        fkroi_c_base = 1
        na_fkroi_c = 1.2

        # 大幅加特惠时，fkroi_bp均值的倍数
        fkroi_bp_base = 1.2

        filter_opti_data_list = []
        # 过滤需要同时考虑city_id和pred_date
        for day in pd.date_range(self.params['step_start_date'], self.params['end_time']).astype(str):
            for city in list(set(opti_data['city_id'])):
                print("city:",city, 'day:',day)

                citydf = roidf.query("city_id ==  %d " % city)  # 城市核销roi数据
                city_opti_data = opti_data.query("city_id == %d & pred_date == '%s'" % (city, day))  # 候选补贴数据
                is_th_pid = 0   # 标记特惠是否命中pid规则
                is_pk_pid = 0   # 标记普快是否命中pid规则

                # 对照组目标补贴率
                # 特惠&普快 呼返处理 如果没有，取0
                thhfdf = ctl_tehuidf.query("city_id == %d & pred_date == '%s'" % (city, day))
                if len(thhfdf) == 1:
                    ctl_thhf = thhfdf.iloc[0].ctl_thhf  # 对照组目标
                else:
                    ctl_thhf = 0
                pkhfdf = ctl_kuaidf.query("city_id == %d & pred_date == '%s'" % (city, day))
                if len(pkhfdf) == 1:
                    ctl_pkhf = pkhfdf.iloc[0].ctl_pkhf
                else:
                    ctl_pkhf = 0
                print("对照组 特惠&普快目标补贴率 特惠呼返:{}, 普快呼返补贴率:{} ".format(ctl_thhf, ctl_pkhf))

                if len(citydf) > 0:
                    # 1、数据获取

                    # 对照组后验数据 & 实验组数据（exp）
                    city_id, thhf, pkhf, throi_c, pkroi_c, throi_bp, pkroi_bp, fkroi_bp, fkroi_c, exp_thhf, exp_pkhf, exp_throi_c = \
                        citydf.iloc[0][['city_id', 'thhf', 'pkhf', 'throi_c', 'pkroi_c', 'throi_bp', 'pkroi_bp', 'fkroi_bp', 'fkroi_c','our_thhf', 'our_pkhf', 'our_throi_c']]

                    print("对照组数据 特惠呼返:{} 特惠ROI:{} 特惠毛利ROI:{} 泛快ROI:{} 泛快毛利ROI:{} 普快呼返:{} 普快ROI:{} 普快毛利ROI:{}".format(thhf,throi_c,throi_bp,fkroi_c, fkroi_bp,pkhf,pkroi_c,pkroi_bp))
                    print("实验组数据 特惠呼返:{} 普快呼返:{} 特惠ROI:{}".format(exp_thhf,exp_pkhf,exp_throi_c))

                    # 2 主要调整逻辑 
                    # 2.1 特惠大幅加预算：
                    # 特惠roi很好, 泛快roi>=均值
                    if round(thhf,2) >= 0.01 and (throi_bp > throi_bp_large_coef * na_throi_bp and throi_c > 0) and \
                        (fkroi_c > fkroi_c_base * na_fkroi_c and (fkroi_bp > fkroi_bp_base * na_fkroi_bp or fkroi_bp < 0)):
                        if abs(thhf - ctl_thhf) >= 0.05:
                            # thhf_lb = max(ctl_thhf + 0.005, (ctl_thhf + thhf) / 2) # 补贴率大于ctl_thhf
                            thhf_lb = (ctl_thhf + thhf) / 2
                        else:
                            thhf_lb = min(max(ctl_thhf + 0.005, thhf), 0.2)
                        # 补贴率上界: 防止加过头
                        if (exp_thhf - thhf) >= 0.01 and exp_throi_c < na_throi_c:  # 实验组加了1pp & 实验组后验特惠c补roi<对照组均值
                            thhf_ub = max((exp_thhf + thhf) / 2, thhf_lb * large_base)  # 防止实验组后验补贴率<对照组
                        else:  # 可以多加点补贴率
                            thhf_ub = max(thhf_lb * large_base, thhf_lb + large_abs, exp_thhf)  # 最多*large_base
                        city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_th>=%.4f and \
                                                            treat_subsidy_hufan_rate_th <= %.4f" % (thhf_lb, thhf_ub))
                        print("add tehui largely:", thhf_lb, thhf_ub)
                        is_th_pid = 1


                    # 特惠大幅加预算：特惠c补roi>0, 毛利roi<0, 泛快roi很好
                    if round(thhf,2) >= 0.01 and (throi_bp < 0 and throi_c > throi_bp_large_coef * na_throi_c) and \
                    (fkroi_c > fkroi_c_base*na_fkroi_c and (fkroi_bp > fkroi_bp_base * na_fkroi_bp  or fkroi_bp < 0)):
                        if abs(thhf - ctl_thhf) >= 0.05:
                            # thhf_lb = max(ctl_thhf + 0.005, (ctl_thhf + thhf) / 2) # 补贴率大于ctl_thhf+0.5pp
                            thhf_lb = (ctl_thhf + thhf) / 2
                        else:
                            thhf_lb = min(max(ctl_thhf + 0.005, thhf), 0.2)
                        # 防止特惠加过头
                        if (exp_thhf - thhf) >= 0.01 and exp_throi_c < na_throi_c:  # 实验组加了1pp & 实验组后验特惠c补roi<对照组均值
                            thhf_ub = max((exp_thhf + thhf) / 2, thhf_lb * large_base)
                        else:
                            thhf_ub = max(thhf_lb * large_base, thhf_lb + large_abs, exp_thhf)  # 最多1.8倍
                        city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_th>=%.4f and \
                                                                        treat_subsidy_hufan_rate_th <= %.4f" % (thhf_lb, thhf_ub))
                        print("add tehui largely:", thhf_lb, thhf_ub)
                        is_th_pid = 1

                    # 特惠中幅加预算：特惠毛利roi较好，泛快roi>=均值
                    if round(thhf,2) >= 0.01 and (throi_c > 0 and throi_bp < throi_bp_large_coef * na_throi_bp \
                        and throi_bp > max(throi_bp_middle_coef * na_throi_bp, na_fkroi_bp)) \
                        and (fkroi_c > fkroi_c_base * na_fkroi_c and (fkroi_bp > fkroi_bp_base * na_fkroi_bp or fkroi_bp < 0)):
                        thhf_lb = min(ctl_thhf, thhf)  # 防止对照组补贴太高，边际roi变得很小的情况
                        thhf_ub = max(thhf_lb * middle_base, thhf_lb + middle_abs)  # 上界起码比下界高6pp
                        city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_th>=%.4f and \
                                                            treat_subsidy_hufan_rate_th<= %.4f" % (thhf_lb, thhf_ub))
                        print("add tehui midly:", thhf_lb, thhf_ub)
                        is_th_pid = 1


                    # 特惠小幅加预算：特惠毛利roi略好，但是泛快毛利roi很好
                    if round(thhf,2) >= 0.01 and (throi_c > 0 and throi_bp > max(throi_bp_small_coef*na_throi_bp, na_fkroi_bp) \
                        and throi_bp < throi_bp_middle_coef * na_throi_bp) \
                        and (fkroi_c > fkroi_c_base * na_fkroi_c and (fkroi_bp > na_fkroi_bp or fkroi_bp < 0)):

                        thhf_lb = min(ctl_thhf, thhf)  # 防止对照组补贴太高，边际roi变得很小的情况
                        thhf_ub = max(thhf_lb * small_base, thhf_lb + small_abs)  # 上界起码比下界高3pp
                        city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_th>=%.4f and \
                                                            treat_subsidy_hufan_rate_th<= %.4f" % (thhf_lb, thhf_ub))
                        print("add tehui little:", thhf_lb, thhf_ub)
                        is_th_pid = 1

                    # 特惠小幅加预算：特惠>泛快均值，泛快roi略好(毛利roi>均值 & < 1.2*均值)
                    if round(thhf,2) >= 0.01 and ((throi_bp > max(throi_bp_small_coef * na_throi_bp, na_fkroi_bp) and throi_c > 0) \
                            or (throi_bp < 0 and throi_c > fkroi_c_base * na_fkroi_c))  and (fkroi_c > fkroi_c_base * na_fkroi_c \
                            and (fkroi_bp > na_fkroi_bp and fkroi_bp < fkroi_bp_base * na_fkroi_bp)):
                        thhf_lb = min(ctl_thhf, thhf)
                        thhf_ub = max(thhf_lb * small_base, thhf_lb + small_abs)  # 上界起码比下界高3pp
                        city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_th>=%.4f and \
                                                                    treat_subsidy_hufan_rate_th<= %.4f" % (thhf_lb, thhf_ub))
                        print("add tehui little:", thhf_lb, thhf_ub)
                        is_th_pid = 1

                    # 特惠减预算：
                    if thhf >= 0.01 and throi_bp < 0 and throi_c < 0:  # 毛利roi<0, c补roi<0
                        city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_th< %.4f-0.01" % ctl_thhf)
                        print("sub tehui:", ctl_thhf)
                        is_th_pid = 1

                    # 特惠不给预算：
                    if thhf < 0.005:
                        city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_th==0")
                        print("no tehui")
                        is_th_pid = 1

                    # 普快加预算：普快roi好，泛快roi好
                    if pkhf > 0 and ((pkroi_bp > na_fkroi_bp and pkroi_c > 0) or (pkroi_bp < 0 and pkroi_c > fkroi_c_base * na_fkroi_c)) \
                        and (fkroi_c > fkroi_c_base * na_fkroi_c and (fkroi_bp > na_fkroi_bp or fkroi_bp < 0)):
                        pkhf_lb = pkhf + 0.005
                        pkhf_ub = min(pkhf_lb * 2, pkhf + 0.02)
                        city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_pk>=%.4f and \
                                                                    treat_subsidy_hufan_rate_pk <= %.4f" % (pkhf_lb, pkhf_ub))
                        print('add pukuai:', pkhf_lb, pkhf_ub)
                        is_pk_pid = 1

                    # 普快减预算：泛快&特惠毛利roi很好，但普快毛利roi很差的城市
                    if pkhf > 0 and ((pkroi_bp > 0 and pkroi_bp < na_fkroi_bp) or (pkroi_bp < 0 and pkroi_c < 0)) \
                        and ((throi_bp > na_fkroi_bp and throi_c > 0) or (throi_bp < 0 and throi_c > fkroi_c_base * na_fkroi_c)) \
                        and (fkroi_c> fkroi_c_base * na_fkroi_c and (fkroi_bp<0 or fkroi_bp > na_fkroi_bp)):
                        pkhf_ub = max(min(ctl_pkhf, pkhf), 0.001)
                        city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_pk < %.4f" % pkhf_ub)
                        print("sub pukuai:", pkhf_ub)
                        is_pk_pid = 1

                    # 普快不给预算
                    if pkhf <= 0:
                        city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_pk==0")
                        print("no pukuai")
                        is_pk_pid = 1

                    # fkroi差：减特惠和普快
                    if thhf > 0 and ((fkroi_bp > 0 and fkroi_bp < na_fkroi_bp) or fkroi_c < fkroi_c_base * na_fkroi_c):
                        thhf_lb = 0
                        thhf_ub = min(ctl_thhf, thhf)
                        pkhf_ub = max(min(ctl_pkhf, pkhf), 0.001)
                        # 如果特惠roi很好，保证特惠下界
                        if thhf >= 0.01 and (throi_bp > na_fkroi_bp and throi_c > 0) or (
                                throi_bp < 0 and throi_c > fkroi_c_base * na_fkroi_c):
                            thhf_lb = thhf_ub * 0.5
                            city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_th<%.4f and \
                                                                treat_subsidy_hufan_rate_th>%.4f" % (thhf_ub, thhf_lb))
                        else:
                            city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_th<%.4f" % thhf_ub)
                        # 减普快预算
                        city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_pk<%.4f" % pkhf_ub)
                        print('sub tehui and sub pukuai:', thhf_lb, thhf_ub, pkhf_ub)
                        is_th_pid = 1
                        is_pk_pid = 1
                
                if is_th_pid == 0:
                    thhf_lb = max(ctl_thhf * 0.5 , 0)
                    thhf_ub = max(ctl_thhf * 1.5, 0.1)
                    city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_th<%.4f and \
                                                                treat_subsidy_hufan_rate_th>%.4f" % (thhf_ub, thhf_lb))
                    print('tehui lack roidf or not hit pid rule:', thhf_lb, thhf_ub)
                if is_pk_pid == 0:
                    pkhf_lb = max(ctl_pkhf * 0.5, 0)
                    pkhf_ub = max(ctl_pkhf * 1.5, 0.05)
                    city_opti_data = city_opti_data.query("treat_subsidy_hufan_rate_pk<%.4f and \
                                                                treat_subsidy_hufan_rate_pk>%.4f" % (pkhf_ub, pkhf_lb))
                    print('pukuai lack roidf or not hit pid rule:', pkhf_lb, pkhf_ub)

                filter_opti_data_list.append(city_opti_data)
        if len(filter_opti_data_list) == 0:
            self.error_callback('C端T+1分配-roi规则过滤后候选集为空!')
        # 生成过滤后的候选集
        filter_opti_data = pd.concat(filter_opti_data_list, ignore_index=True)

        # 保证top 50城市价差
        filter_opti_data['treat_subsidy_hufan_rate_pk'] = filter_opti_data['treat_subsidy_hufan_rate_pk'].apply(lambda x: round(x, 3))
        filter_opti_data['treat_subsidy_hufan_rate_th'] = filter_opti_data['treat_subsidy_hufan_rate_th'].apply(lambda x: round(x, 3))
        filter_opti_data1 = decay_price_gap_for_lackcity(filter_opti_data, topcitystr) # 价差衰减函数
        # 非top 50城市，没有价差限制
        filter_opti_data2 = filter_opti_data.query("city_id not in (%s)" % topcitystr)
        new_filter_opti_data = pd.concat([filter_opti_data1, filter_opti_data2], ignore_index=True)

        # 候选集过滤后，缺失城市follow对照组
        top_city_list = list(map(int, topcitystr.split(',')))
        lackdf_li = []
        for day in pd.date_range(self.params['step_start_date'], self.params['end_time']).astype(str):
            lackcities = set(opti_data.query("pred_date=='%s'" % day).city_id.unique()) - \
                        set(new_filter_opti_data.query("pred_date=='%s'" % day).city_id.unique())
            print("day:",day, 'lackcities:', lackcities)
            print("top 50 lackcity:", sorted(list(set(top_city_list) & set(lackcities))))
            ctldf = pd.merge(ctl_tehuidf, ctl_kuaidf, on=['city_id','pred_date'],how='outer')
            ctldf = ctldf.query("city_id in @lackcities & pred_date=='%s'" % day) # 缺失城市
            ctldf.replace({np.nan: 0, np.inf: 0, -np.inf: 0}, inplace=True)

            opdata = opti_data[['city_id', 'pred_date', 'gmv_k', 'gmv_pk', 'gmv_th']].drop_duplicates()
            ctldf = pd.merge(ctldf, opdata, on=['city_id','pred_date'], how='left')
            print("ctldf len:", len(ctldf))

            # 对照组thhf也要是0.5pp的整数倍, 否则补贴率可能不在核销字典中
            ctldf['ctl_thhf'] = round(ctldf['ctl_thhf'], 2)
            ctldf['ctl_pkhf'] = round(ctldf['ctl_pkhf'], 2)

            ctldf['budget_k'] = ctldf['gmv_k'] * ctldf['ctl_pkhf']
            ctldf['budget_pk'] = ctldf['gmv_pk'] * ctldf['ctl_pkhf']
            ctldf['budget_th'] = ctldf['gmv_th'] * ctldf['ctl_thhf']
            lackdf = ctldf.rename(columns={'ctl_thhf': 'treat_subsidy_hufan_rate_th', 'ctl_pkhf': 'treat_subsidy_hufan_rate_pk'})
            lackdf['d_gmv_k']=0
            lackdf['d_gmv_th']=0
            lackdf['pred_date'] = day
            lackdf_li.append(lackdf)
        lackdf = pd.concat(lackdf_li, ignore_index=True)
        new_filter_opti_data = pd.concat([new_filter_opti_data, lackdf],ignore_index=True)
        # pkhf向下兼容核销
        new_filter_opti_data['treat_subsidy_hufan_rate_pk'] = new_filter_opti_data.apply(utils.fix_pkrate_func,axis=1, args=(hexiao_ratedict,))
        new_filter_opti_data['budget_pk'] = new_filter_opti_data['treat_subsidy_hufan_rate_pk'] * new_filter_opti_data['gmv_pk']
        new_filter_opti_data['budget_k'] = new_filter_opti_data['treat_subsidy_hufan_rate_pk'] * new_filter_opti_data['gmv_k']

        # 防止重复候选集
        new_filter_opti_data2 = new_filter_opti_data.drop_duplicates(subset=['city_id', 'pred_date', 'treat_subsidy_hufan_rate_pk', 'treat_subsidy_hufan_rate_th'])
        print("兼容补贴率字典前，候选集：", len(new_filter_opti_data), "兼容后：", len(new_filter_opti_data2))

        filter_opti_sp = self.hc.createDataFrame(new_filter_opti_data2)
        filter_opti_sp.repartition(10).write.mode('overwrite').csv("filter_opti_df_step_start_date_%s_end_date_%s"
                                        % (self.params['step_start_date'], self.params['end_time']), header=True)
        return new_filter_opti_data2


    def get_global_infos(self):
        '''
        get all the input and param we need
        :return:
        '''
        cur_day = self._stg_param['trigger_time'][0:10]
        trace_id = self._stg_param['trace_id']
        rely_info = self._stg_param['rely_info']

        start_date = self._stg_param['start_date']
        end_date = self._stg_param['end_date']
        step_start_date = self._stg_param['step_start_date']
        step_end_date = self._stg_param['step_end_date']

        total_budget = self._stg_param['budget']
        cost = self._stg_param['cost']

        version_list = self._stg_param['version_list']
        child_info = self._stg_param['child_info']

        ext_data = self._stg_param['ext_data'] # 包含不同城市、生效时间、产品线、caller   
        step_id = self._stg_param['step_id']
        step_type = self._stg_param['step_type']
        order_id = self._stg_param['order_id']  

        gap_rate = get_gap_rate()
        accelerate_card_rate_dict, pangu_rate_dict = get_b_rate_dict()

        return cur_day, trace_id, rely_info, start_date, end_date, step_start_date, step_end_date, total_budget, cost,  version_list, child_info, ext_data, gap_rate, accelerate_card_rate_dict, pangu_rate_dict,step_id,order_id,step_type



    def get_b_fillup_optimal_data(self, gap_dignosis, acc_elastic, pangu_elastic, forecast_daily_data, asp_df, type_version,max_try_num=7):

        """
        获取B端填坑活动分配信息
        """
        # 1、加速卡填坑： 首先需要检查加速卡是否存在活动，如果不存在，直接进行盘古填坑，如果存在，需要先进行加速卡填坑
        acc_elastic_merge_df = pd.merge(gap_dignosis[gap_dignosis['tool'] =='加速卡'],acc_elastic[acc_elastic['b_rate'] != 0], on = ['city_id','stat_date','tool'],how ='inner') 
        if acc_elastic_merge_df.shape[0] != MIN_NUM_OF_DATA:
            # acc_elastic_merge_df = pd.merge(gap_dignosis[gap_dignosis['tool'] =='加速卡'],acc_elastic[acc_elastic['b_rate'] != 0], on = ['city_id','stat_date','tool'],how ='inner') 
            acc_elastic_merge_df = acc_elastic_merge_df[ acc_elastic_merge_df['stat_hour'] >= acc_elastic_merge_df['start_hour']][acc_elastic_merge_df['stat_hour'] <= acc_elastic_merge_df['end_hour'] ]
            acc_elastic_merge_df[['gmv_h' ,'call_order_cnt_h','delta_cr_h','delta_ride_h', 'abn_rate_h','subsidy_h','cr_gap_h']] = acc_elastic_merge_df.apply(self.get_hourly_indicator,axis=1,result_type = 'expand')
            #  中间产物，通过该表获取加速卡活动后分小时的cr变化
            acc_hourly_delta_cr = acc_elastic_merge_df[['city_id','stat_date','fence_id','tool','stat_hour','cr','cr_bar','delta_cr_h']] 
            # 聚合到活动粒度,和盘古规划活动一起进行join，作为最后输出
            acc_event_info =  acc_elastic_merge_df.groupby(['city_id','stat_date','fence_id','start_time','end_time','cr','cr_bar','b_rate'],as_index = False).agg(
                {'gmv_h':'sum',
                'abn_rate_h':'sum',
                'subsidy_h':'sum',
                'cr_gap_h':'mean',
                'delta_ride_h':'sum'
                }).rename(columns = {'gmv_h':'gmv','abn_rate_h':'abn_rate','subsidy_h':'subsidy','cr_gap_h':'cr_gap','delta_ride_h':'delta_ride'})
            acc_event_info['tool'] = '加速卡'

            print("加速卡活动数: {} ,加速卡填坑花费:{} ".format(acc_event_info.shape, acc_event_info['subsidy'].sum()))
        else:
            acc_hourly_delta_cr = pd.DataFrame(columns=['city_id','stat_date','fence_id','tool','stat_hour','cr','cr_bar','delta_cr_h'])
            acc_event_info = pd.DataFrame(columns=['city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr',
                            'cr_bar', 'b_rate', 'gmv', 'abn_rate','delta_ride', 'subsidy', 'tool'])

            print("加速卡活动数: 0 ,加速卡填坑花费: 0.0 ")

        # 2、盘古填坑
        # 首先加工盘古活动的分小时弹性数据
        print(gap_dignosis.head())
        print(pangu_elastic.head())
        gap_dignosis = gap_dignosis.astype({'city_id':'int','stat_date':'str'})
        pangu_elastic = pangu_elastic.astype({'city_id':'int','stat_date':'str'})
        pangu_elastic_merge_df = pd.merge(gap_dignosis[gap_dignosis['tool'] =='智能盘古'],pangu_elastic, on = ['city_id','stat_date','tool'],how ='inner')
        pangu_elastic_merge_df  = pangu_elastic_merge_df[ pangu_elastic_merge_df['stat_hour'] >= pangu_elastic_merge_df['start_hour']][pangu_elastic_merge_df['stat_hour'] <= pangu_elastic_merge_df['end_hour'] ]

        pangu_elastic_merge_df = pangu_elastic_merge_df.drop( pangu_elastic_merge_df[pangu_elastic_merge_df['end_hour'] - pangu_elastic_merge_df['start_hour'] >=8 ][pangu_elastic_merge_df['b_rate'] >=0.08].index )  
        pangu_elastic_merge_df["duration_check"] = pangu_elastic_merge_df.apply(lambda x : short_duration_check(x.start_time, x.end_time, x.b_rate),axis=1 , result_type = 'expand')
        print(pangu_elastic_merge_df.query("duration_check == 1 "))
        pangu_elastic_merge_df = pangu_elastic_merge_df.drop( pangu_elastic_merge_df[pangu_elastic_merge_df['duration_check'] == 1].index)

        pangu_merge_acc_cr  = pd.merge(pangu_elastic_merge_df,acc_hourly_delta_cr , on = ['city_id','stat_date','stat_hour'], how = 'left',suffixes=('', '_acc'))
        pangu_merge_acc_cr['delta_cr_h'] = pangu_merge_acc_cr['delta_cr_h'].fillna(0)
        pangu_merge_acc_cr['cr_ori'] = pangu_merge_acc_cr['cr']  
        pangu_merge_acc_cr['cr'] = pangu_merge_acc_cr['cr_ori'] + pangu_merge_acc_cr['delta_cr_h']   # cr update  
        
        print("pangu_merge_acc_cr columns ",pangu_merge_acc_cr.columns)
        
        df = pangu_merge_acc_cr.drop(labels=['fence_id_acc','fence_id_acc','tool_acc','cr_acc','cr_bar_acc','delta_cr_h'], axis=1)
        if df.shape[0] != 0:
            # 指标加工
            df[['gmv_h' ,'call_order_cnt_h','delta_cr_h','delta_ride_h', 'abn_rate_h','subsidy_h','cr_gap_h']] = df.apply(self.get_hourly_indicator,axis=1,result_type = 'expand')
            # 聚合到活动粒度,然后进行规划
            event_info =  df.groupby(['city_id','stat_date','fence_id','start_time','end_time','cr','cr_bar','b_rate'],as_index = False).agg(
                {'gmv_h':'sum',
                'abn_rate_h':'sum',
                'subsidy_h':'sum',
                'cr_gap_h':'mean',
                'delta_ride_h':'sum'
                }).rename(columns = {'gmv_h':'gmv','abn_rate_h':'abn_rate','subsidy_h':'subsidy','cr_gap_h':'cr_gap','delta_ride_h':'delta_ride'})
            
            event_info['tool'] = '智能盘古'
            print("盘古活动数: {} ,盘古填坑花费:{} ".format(event_info.shape, event_info['subsidy'].sum()))
            print("盘古填坑分布:",event_info.groupby("stat_date").count())

            # 3、填坑规划求解
            gap_pred_info = event_info.reset_index(drop = True)
            gap_pred_info['idx'] = list(range(gap_pred_info.shape[0]))
            gap_pred_info['group_idx'] = ['_'.join(i) for i in gap_pred_info[['start_time','city_id','fence_id','stat_date']].values.astype(str)] # 添加唯一idx ，作为该区县时段唯一标识
            print("盘古填坑规划准备数据 info:",gap_pred_info.shape,gap_pred_info.head())

            gap_choose_scheme = self.gap_find_best_option(gap_pred_info,acc_event_info['subsidy'].sum())
        else:
            gap_choose_scheme = pd.DataFrame(columns=['city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr',
                            'cr_bar', 'b_rate', 'gmv', 'abn_rate','delta_ride', 'subsidy', 'tool'])

        # 4、加速卡和盘古结果融合

        gap_choose_scheme = gap_choose_scheme[gap_choose_scheme['subsidy'] != 0] # 取实际花钱的活动
        if acc_elastic.shape[0] != MIN_NUM_OF_DATA:
            gap_choose_scheme = pd.concat([gap_choose_scheme[acc_event_info.columns],acc_event_info])
        else:
            gap_choose_scheme = gap_choose_scheme[['city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr',
                                'cr_bar', 'b_rate', 'gmv', 'abn_rate', 'subsidy', 'tool', 'delta_ride']]
        print("加速卡和盘古结果活动总数：",gap_choose_scheme.shape)

        # 20220616 最终的补贴活动信息 dashboard_v2
        if gap_choose_scheme.shape[0] != MIN_NUM_OF_DATA:
            daily_gmv_info = forecast_daily_data[['city_id','stat_date','total_gmv']].rename(columns = {'total_gmv':'gmv_daily'})
            daily_gmv_info.drop_duplicates(subset=['city_id','stat_date','gmv_daily'],keep='first',inplace=True)
            # 绘制补贴率和坑深之间的关系 需要除去加速卡影响活动
            b_allocation_event_link, gap_brate_event_link, abn_event_link,abn_event_len = allocation_info_check(gap_choose_scheme, daily_gmv_info)
            if abn_event_len == MIN_NUM_OF_DATA:
                title = "T+N分配-活动补贴信息-无异常补贴活动"
                report_message( title ,self._stg_param['is_online'], "T+N-"+type_version+"-活动补贴信息",b_allocation_event_link, "", gap_brate_event_link)
            else:
                title = "T+N分配-活动补贴信息-发现{}个异常补贴活动,详情如下".format(abn_event_len)
                report_message( title ,self._stg_param['is_online'], "T+N-"+type_version+"-活动补贴信息",b_allocation_event_link, abn_event_link, gap_brate_event_link)
           
        if gap_choose_scheme.shape[0] != 0:
            gap_choose_scheme[["pangu_stg_detail","acc_card_stg_detail"]] = gap_choose_scheme.apply(self.agg_event_2_city_options,axis=1,result_type = 'expand')
            # 聚合到天
            gap_b_table = self.return_b_gap_info(gap_choose_scheme,forecast_daily_data, asp_df)
            rides_increase = gap_b_table['delta_ride'].sum()

            gap_b_table = gap_b_table[['city_id','pred_date','b_budget','acc_budget','pangu_stg_detail','acc_card_stg_detail','delta_ride','gmv','asp','delta_gmv']]
        else:
            gap_b_table = pd.DataFrame(columns=['city_id','pred_date','b_budget','acc_budget','pangu_stg_detail','acc_card_stg_detail','delta_ride','gmv','asp','delta_gmv'])
        print("填坑结果-gap_b_table:",gap_b_table.head())

        # 20220616 最终的补贴城市信息 dashboard_v2
        if gap_b_table.shape[0]!= MIN_NUM_OF_DATA :
            b_allocation_daily_link = allocation_info_daily_check(gap_b_table,daily_gmv_info)
            report_message("T+N分配-城市补贴信息",self._stg_param['is_online'],"T+N-"+type_version+"-城市补贴信息", b_allocation_daily_link,"","")

        '''
        schema = StructType([StructField("city_id", IntegerType(), True),  StructField("pred_date", StringType(),True ), StructField("b_budget", DoubleType(), True),StructField("acc_budget", DoubleType(), True), \
                        StructField("pangu_stg_detail", StringType(), True),StructField("acc_card_stg_detail", StringType(), True)])
        gap_b_table_df = self.hc.createDataFrame(gap_b_table ,schema = schema)
        '''
        return gap_b_table


    def get_forecast_data_for_b_fillup(self,df_sample):

        if self.params['cr_type'] == 'objective_exp_openapi_pp':
            print("SQL of forecast daily for b fillup sql: ",forecast_daily_for_b_fillup_sql.format(**self.params))
            forecast_data_raw = self.hc.sql(forecast_daily_for_b_fillup_sql.format(**self.params)).toPandas()
        else:
            print("SQL of exp forecast daily for b fillup sql: ",exp_forecast_daily_for_b_fillup_sql.format(**self.params))
            forecast_data_raw = self.hc.sql(exp_forecast_daily_for_b_fillup_sql.format(**self.params)).toPandas()
        forecast_data = pd.merge(forecast_data_raw,df_sample,on=['stat_date','city_id'],how = 'right').reset_index().astype({'call_order_cnt':'float',
                                                                                                                            'total_gmv':'float',
                                                                                                                            'total_finish_order_cnt':'float'})  # 引入duplicate ，df_sample tool 造成的
        # 防止分母为0
        forecast_data['call_order_cnt'] = forecast_data['call_order_cnt'].fillna(1)
        # 防止接近0的完单造成计算cr时大于1
        forecast_data['call_order_cnt'] = forecast_data['call_order_cnt'] + 1
        
        forecast_data['total_gmv'] = forecast_data['total_gmv'].fillna(1)
        # 有非常小的值，小数点四舍五入之后会变为0，整体加0.1
        forecast_data['total_gmv'] = forecast_data['total_gmv'] + 0.1 
        
        #print(forecast_data)

        if forecast_data.shape[0] != MIN_NUM_OF_DATA :
            return forecast_data
        else:
            self.error_callback('B端周分配-获取天级别供需数据失败!')

    def extract_json_format_b_budget(self, df, strategy_type_str, forecast_daily_data):
        b_budget_dict = {}
        rely_info = {}
        rely_info['gmv_dt'] = self.params['gmv_dt']
        cr = int(strategy_type_str[-2:])/100
        rely_info['cr'] = str(cr)
        rely_info['cr_dt'] = self.params['cr_dt']
        rely_info['cr_type'] = self.params['cr_type']
        for version in json.loads(self.params['version_list']):
            if version['cr'] == cr:
                v = version['version_code']
                break
        b_budget_dict['rely_info'] = rely_info
        b_budget_dict['version'] = v
        delta_gmv_all = df['delta_gmv'].sum()

        forecast_daily_data['is_zero'] = 1
        data = []
        for index,city_day_df in df.iterrows():
            if city_day_df['acc_budget'] != 0:
                city_day_data_acc_dict = {}
                city_day_data_acc_dict['city'] = city_day_df['city_id']
                city_day_data_acc_dict["product_line"] = "kuaiche"
                city_day_data_acc_dict["caller"] = "accelerate_card"
                city_day_data_acc_dict['stat_date'] = city_day_df['pred_date']
                city_day_data_acc_dict['stg_detail'] = city_day_df['acc_card_stg_detail']
                city_day_data_acc_dict['amount'] = city_day_df['acc_budget']    
                city_day_data_acc_dict['gmv'] = city_day_df['gmv']
                city_day_data_acc_dict['step_type'] = self.params['step_type']
                data.append(city_day_data_acc_dict)
                forecast_daily_data.loc[(forecast_daily_data['city_id'] == city_day_df['city_id']) & (forecast_daily_data['stat_date'] == city_day_df['pred_date']) & (forecast_daily_data['tool'] == '加速卡'),'is_zero'] = 0
                
            if city_day_df['b_budget'] != 0:
                city_day_data_pangu_dict = {}
                city_day_data_pangu_dict['city'] = city_day_df['city_id']
                city_day_data_pangu_dict["product_line"] = "kuaiche"
                city_day_data_pangu_dict["caller"] = "b"
                city_day_data_pangu_dict['stat_date'] = city_day_df['pred_date']
                city_day_data_pangu_dict['stg_detail'] = city_day_df['pangu_stg_detail']
                city_day_data_pangu_dict['amount'] = city_day_df['b_budget']        
                city_day_data_pangu_dict['gmv'] = city_day_df['gmv']
                city_day_data_pangu_dict['step_type'] = self.params['step_type']
                data.append(city_day_data_pangu_dict)
                forecast_daily_data.loc[(forecast_daily_data['city_id'] == city_day_df['city_id']) & (forecast_daily_data['stat_date'] == city_day_df['pred_date']) & (forecast_daily_data['tool'] == '智能盘古'),'is_zero'] = 0
        
        df_sample_ans_res = forecast_daily_data[forecast_daily_data['is_zero'] == 1]
        for index,city_day_df_sample in df_sample_ans_res.iterrows():
            if city_day_df_sample['tool'] == '智能盘古':
                city_day_data_pangu_dict = {}
                city_day_data_pangu_dict['city'] = city_day_df_sample['city_id']
                city_day_data_pangu_dict["product_line"] = "kuaiche"
                city_day_data_pangu_dict["caller"] = "b"
                city_day_data_pangu_dict['stat_date'] = city_day_df_sample['stat_date']
                city_day_data_pangu_dict['stg_detail'] = []
                city_day_data_pangu_dict['amount'] = 0      
                city_day_data_pangu_dict['gmv'] = round(city_day_df_sample['total_gmv'],4)
                city_day_data_pangu_dict['step_type'] = self.params['step_type']
                data.append(city_day_data_pangu_dict)
            if city_day_df_sample['tool'] == '加速卡':
                city_day_data_acc_dict = {}
                city_day_data_acc_dict['city'] = city_day_df_sample['city_id']
                city_day_data_acc_dict["product_line"] = "kuaiche"
                city_day_data_acc_dict["caller"] = "accelerate_card"
                city_day_data_acc_dict['stat_date'] = city_day_df_sample['stat_date']
                city_day_data_acc_dict['stg_detail'] = []
                city_day_data_acc_dict['amount'] = 0
                city_day_data_acc_dict['gmv'] = round(city_day_df_sample['total_gmv'],4)
                city_day_data_acc_dict['step_type'] = self.params['step_type']
                data.append(city_day_data_acc_dict)

        print('df_sample len:',len(forecast_daily_data))
        print('return data:',len(data))
        b_budget_dict['data'] = data
        ext_data = {}
        # 计算周成交率cr(总体cr和分城市的cr)
        df_day = df.groupby('city_id').agg({'delta_ride':'sum'}).reset_index()
        
        base_call_finsh = forecast_daily_data.groupby('city_id').agg({'total_finish_order_cnt':'sum','call_order_cnt':'sum'}).reset_index()
        #print('base_call_finsh')
        #print(base_call_finsh)
        df_day.set_index('city_id', inplace=True)
        base_call_finsh.set_index('city_id', inplace=True)

        call_finish_rides = base_call_finsh.join(df_day,on = 'city_id').fillna(0)
        #print('call_finish_rides')
        #print(call_finish_rides)
        call_finish_rides = call_finish_rides.reset_index()
        call_finish_rides = call_finish_rides.astype({'delta_ride':'float','total_finish_order_cnt':'float','call_order_cnt':'float','city_id':'int'})

        call_finish_rides['cr_city'] = (call_finish_rides['delta_ride']+call_finish_rides['total_finish_order_cnt'])/call_finish_rides['call_order_cnt']
        #print(call_finish_rides)
        city_cr_aft_sub = dict(zip(call_finish_rides['city_id'],round(call_finish_rides['cr_city'],4)))
        print(call_finish_rides['delta_ride'].sum())
        print(call_finish_rides['total_finish_order_cnt'].sum())
        print(call_finish_rides['call_order_cnt'].sum())
        cr_aft_sub = (call_finish_rides['delta_ride'].sum()+call_finish_rides['total_finish_order_cnt'].sum())/call_finish_rides['call_order_cnt'].sum()
        
        city_cr_aft_sub['0'] = round(cr_aft_sub,4)
        ext_data['sum_cr_rate'] = city_cr_aft_sub
        ext_data['delta_gmv'] = round(delta_gmv_all,4)

        b_budget_dict['ext_data'] = ext_data

        return b_budget_dict


    def get_history_diaodu_scope(self, max_try_num=7):
        for try_num in range(max_try_num):
            print('try_num:',try_num)
            self.params['try_num'] = try_num
            print(diaodu_sql.format(**self.params))
            pred_diaodu_info = self.hc.sql(diaodu_sql.format(**self.params))
            if(pred_diaodu_info.count()>0):
                pred_diaodu_info.registerTempTable('pred_diaodu_info')
                return pred_diaodu_info 
        if(self.params['try_num'] == max_try_num-1):
            self.error_callback('获取调度数据失败 !')
        
    def c_table_merge_b_table(self, c_data):
        # 得到b端月级别填坑表。
        b_table = self.hc.createDataFrame(self.get_bn_budget_vertify())
        print('筛选后的B端填坑规划结果 count and head 10: ', b_table.count())
        print(b_table.show(10))
        if b_table.count() == 0:
            self.params['b_spent'] = 0 
        else:
            self.params['b_spent'] =  b_table.agg({'ori_b_budget': 'sum'}).collect()[0]['sum(ori_b_budget)'] 
        # 和c表拼接
        print(c_data.show(10))
        c_b_data = c_data.join(b_table, (c_data['city_id'] == b_table['city_id']) & 
                                        (c_data['pred_date'] == b_table['pred_date']), 'left').drop(b_table['city_id']).drop(b_table['pred_date'])\
                                            .withColumn('ori_b_rate', (col('ori_b_budget')/col('total_gmv') * 1000).cast("int")).withColumn('ori_acc_rate', ( 0 /col('total_gmv') * 1000).cast("int"))  # 生成对应原始补贴率
        c_b_data = c_b_data.na.fill(subset='ori_b_budget', value=0.0)
        c_b_data = c_b_data.na.fill(subset='ori_b_rate', value=0.0)
        c_b_data = c_b_data.na.fill(subset='ori_acc_budget', value=0.0)
        c_b_data = c_b_data.na.fill(subset='ori_acc_rate', value=0.0)
        print(" After merge B&C, and filter all null data , any null data bellow : ",c_b_data.filter('ori_b_budget is null ').show())
        print("************合并BC表以后，结果如下,接下来合并B表弹性************") 
        print(c_b_data.show(5))
        # 拿到弹性结果
        b_elasticity_df = self.get_b_elasticity() #TODO 如果B端填坑的预算为空，那么对应的b端弹性也一定是0（只在坑里面进行平衡预算分配）
        # b_elasticity_df.write.mode('overwrite').saveAsTable('bigdata_driver_ecosys_test.temp_b_elasticity_df')
        # join b 端弹性，主要有3种情况：1、gap and ballance 补贴率相同，也就是ballance 没钱，2、新的补贴率为0的 3、gap有补贴，保证新的补贴率大于gap 补贴
        c_b_data = c_b_data.join(b_elasticity_df, (c_b_data['city_id'] == b_elasticity_df['city_id']) &
                                                  ((c_b_data['ori_b_rate'] == b_elasticity_df['new_b_rate']) |
                                                  (0.0 == b_elasticity_df['new_b_rate']) 
                                                  ), 'left').drop(b_elasticity_df['city_id'])
        c_b_data = c_b_data.na.fill(subset='delta_tsh', value=0.0)
        def get_new_b_rate(ori_b_rate, new_b_rate):
            if new_b_rate is None:
                return ori_b_rate
            else:
                return new_b_rate
        get_new_b_rate_f = udf(get_new_b_rate, IntegerType())
        c_b_data = c_b_data.withColumn('new_b_rate', get_new_b_rate_f(col('ori_b_rate'), col('new_b_rate')))
        print("c_b_data.show()",c_b_data.show(5))
        def get_realloc_budeget_b(total_gmv, ori_b_budget, ori_b_rate, new_b_rate):
            if ori_b_rate == new_b_rate:
                return 0.0
            return 0.0
        get_realloc_budeget_b_f = udf(get_realloc_budeget_b, DoubleType()) 
        c_b_data = c_b_data.withColumn('realloc_b_budget', get_realloc_budeget_b_f(col('total_gmv'),
                                                                                   col('ori_b_budget'),
                                                                                   col('ori_b_rate'),
                                                                                   col('new_b_rate')))
        c_b_data.registerTempTable('ori_table_ori')
        c_b_data = c_b_data.filter("realloc_b_budget >= 0 ")
        c_b_data.registerTempTable('ori_table')
        print('b and c merge!, count:', c_b_data.count())
        print(c_b_data.show(2))
        return c_b_data


    def merge_all_pre(self):
        # 1. 获取C端和调度合并数据
        c_data, c_ori = self.get_c_els_merge_dd_tab()
        c_data = c_data.na.fill(subset='treat_subsidy_hufan_rate_new', value=0.0)
        c_data = c_data.na.fill(subset='pred_dsend_ratio', value=0.0)
        c_data = c_data.na.fill(subset='realloc_budget_c', value=0.0)
        c_data = c_data.na.fill(subset='subsidy_money_lowerbound', value=0.0)
        c_data = c_data.na.fill(subset='subsidy_money_upperbound', value=0.0)
        c_data = c_data.na.fill(subset='lastweek_avg_diaodu_rate', value= 0.0)
        c_data = c_data.na.fill(subset='lastweek_diaodu_gmv', value=1e-14)
        print('*********c_data.filter_pred_dsend_ratio_is_null**********')
        c_data.filter('pred_dsend_ratio is null').show()

        print("打印参数：",self.params)
        # 2. 拿到b端填坑表和b端弹性。 todo 修改b端填坑
        c_b_data = self.c_table_merge_b_table(c_data)
        
        # 3. 计算完单率和dgmv
        choose_rows = self.process_c_b_table()
        choose_rows = choose_rows.filter("((ori_b_budget!= 0) and ( (new_c_rate <= 21) or (new_c_rate-ori_c_rate < 5 ) ) )\
                                           or (ori_b_budget == 0) ")

        return choose_rows

    def get_c_els_merge_dd_tab(self):
        '''
        :return:  C端填坑弹性表 C端填坑表
        '''
        # 1.C端base表
        print(pred_and_c_sql.format(**self.params))
        c_rate = self.hc.sql(pred_and_c_sql.format(**self.params))
        # print("C端base表",c_rate.show(5))

        # 2. 弹性表
        month_level_c_delta_send_rate_df = self.get_c_elasticity_ori()
        print("C端弹性表数据")
        print(month_level_c_delta_send_rate_df.show(5))
        print('c elasticity count:', month_level_c_delta_send_rate_df.count())
        # 3. base表和弹性表拼接
        c_data = c_rate.join(month_level_c_delta_send_rate_df, (c_rate['city_id'] == month_level_c_delta_send_rate_df['city_id']) &
                             (c_rate['pred_date'] == month_level_c_delta_send_rate_df['pred_date']) &
                             ((c_rate['treat_subsidy_hufan_rate'] == month_level_c_delta_send_rate_df['treat_subsidy_hufan_rate_new']) |
                              (0.0 == month_level_c_delta_send_rate_df['treat_subsidy_hufan_rate_new']) |
                              ((c_rate['treat_subsidy_hufan_rate'] <= month_level_c_delta_send_rate_df['treat_subsidy_hufan_rate_new']) &
                               (month_level_c_delta_send_rate_df['treat_subsidy_hufan_rate_new'] <= 39) &
                                 (month_level_c_delta_send_rate_df['treat_subsidy_hufan_rate_new'] % 3 == 0)
                               )
                              ), 'left')\
            .drop(month_level_c_delta_send_rate_df['city_id']).drop(month_level_c_delta_send_rate_df['pred_date'])
        # print('c candidate number:', c_data.count())
        c_data = c_data.withColumn('realloc_budget_c', col(
            'pukuai_gmv')/1000 * (col('treat_subsidy_hufan_rate_new')-col('treat_subsidy_hufan_rate')))

        # 4 得到历史调度上下区间
        history_diaodu_scope = self.get_history_diaodu_scope()
        c_data = c_data.join(history_diaodu_scope,(c_data['city_id'] == history_diaodu_scope['city_id']) &
                                              (c_data['day_of_week'] == history_diaodu_scope['day_of_week'])
                                             ,'left'
                        ).drop(history_diaodu_scope['city_id']).drop(history_diaodu_scope['day_of_week'])
        print('C端填坑合并弹性表结果数据')
        print(c_data.show(10))

        print('调度结果修正，修正过高的预算')      
        def constrain_bj_temp(subsidy_money_upperbound):
            if subsidy_money_upperbound > DIAODU_SUBSIDY_UPBOUND:
                subsidy_money_upperbound = DIAODU_SUBSIDY_UPBOUND
                return subsidy_money_upperbound
            else:
                return subsidy_money_upperbound
        constrain_bj_temp_f = udf(constrain_bj_temp, DoubleType())
        c_data = c_data.withColumn('subsidy_money_upperbound', constrain_bj_temp_f(col('subsidy_money_upperbound')))
        print(c_data.show(100))

        return c_data, c_rate

    def process_c_b_table(self):
        pred_info = self.hc.sql(sum_sql.format(**self.params)) 
        pred_info = pred_info.na.fill(subset='k1', value=0.0)
        pred_info = pred_info.na.fill(subset='k2', value=0.0)
        pred_info = pred_info.na.fill(subset='k3', value=0.0)
        pred_info = pred_info.na.fill(subset='alpha', value=0.0)
        pred_info = pred_info.na.fill(subset='intercept', value=0.0)
        pred_info = pred_info.na.fill(subset='gongxu', value=0.0)
        pred_info = pred_info.na.fill(subset='gongxu_max', value=1.0)
        pred_info = pred_info.na.fill(subset='gongxu_min', value=0.0)
        pred_info = pred_info.na.fill(subset='diaodu_b_max', value= 1.0)
        pred_info = pred_info.na.fill(subset='diaodu_b_min', value=0.0)
    
        def cal_succ_rate(infos,total_columns):
            gongxu_max,gongxu_min = infos.gongxu_max, infos.gongxu_min
            diaodu_b_max,diaodu_b_min = infos.diaodu_b_max, infos.diaodu_b_min

            diaodu_min_val, diaodu_max_val = infos.subsidy_money_lowerbound, infos.subsidy_money_upperbound
            gongxu, total_gmv = infos.gongxu, infos.total_gmv
            if diaodu_max_val - diaodu_min_val < 1: #equal, reset min, max
                if diaodu_max_val > 0:
                    diaodu_min_val = 0.7 * diaodu_max_val  # 如果下限高于上限，且上限大于0 ，则将下限置为0.7 * 上限
                else:
                    diaodu_max_val = 1000.0 
                    diaodu_min_val = 0.0
            
            gongxu_normalization = (gongxu - gongxu_min)/(gongxu_max - gongxu_min)
            
            type,k1,k2,k3 = infos.type, infos.k1,infos.k2,infos.k3
            alpha, intercept = infos.alpha,infos.intercept
            
            outputs = []
            ori_row = []
            for i in total_columns:
                ori_row.append(infos[i])
            a = np.linspace(diaodu_min_val, diaodu_max_val,10)
            # 20220818 去除调度 0.0, 已经没有屏蔽策略
            for cur_td_subsidy in sorted( list(np.linspace(diaodu_min_val, diaodu_max_val,10))):
                cur_td_subsidy = float(cur_td_subsidy)
                if total_gmv != 0.0 :
                    cur_dd_rate = cur_td_subsidy * 1.0 / total_gmv
                else:
                    cur_dd_rate = 0.0 # TODO 临时修改 20211228
                cur_dd_normalization_rate = (cur_dd_rate - diaodu_b_min) / (diaodu_b_max - diaodu_b_min) 
                if cur_dd_normalization_rate > 1:
                    cur_dd_normalization_rate = 1.0
                if cur_dd_normalization_rate < 0.0:
                    cur_dd_normalization_rate = 0.0
                if gongxu_normalization > 1.0:
                    gongxu_normalization = 1.0
                if gongxu_normalization < 0.0:
                    gongxu_normalization = 0.0
                if type == 'sigmoid_linear':
                    succ_rate =  (1.0) / (1 + np.exp(-k1*gongxu_normalization - k2 * cur_dd_normalization_rate +k3 ))
                    succ_rate = float(succ_rate)
                elif type == 'ElasticNet':
                    succ_rate = k1 * gongxu_normalization + k2 * cur_dd_normalization_rate + k3
                elif type == 'orgfinish':
                    succ_rate = float(np.exp(alpha * gongxu + intercept)/(1 + np.exp(alpha * gongxu + intercept)))
                else:
                    succ_rate = 0.0
                outputs.append(ori_row + [gongxu_normalization,cur_dd_normalization_rate,cur_td_subsidy,succ_rate])   
                #TODO 定位 cur_diaodu_subsidy 同为0，但是cur_td_subsidy不同
                
            return outputs

        total_columns = pred_info.columns
        df_process = pred_info.na.fill(subset = 'new_call_order_cnt', value = 0.0)
        df_process = pred_info.na.fill(subset = 'asp', value = 0.0)
        df_process = pred_info.rdd.flatMap(lambda x: cal_succ_rate(x,total_columns))\
                       .toDF(total_columns+['gongxu_normalization','cur_diaodu_normalization_rate','cur_diaodu_subsidy','succ_rate'])
        df_process = df_process.withColumn('gmv', col('new_call_order_cnt') * col('succ_rate') * col('asp')).cache() 
        df_process = df_process.na.fill(subset = 'gmv', value = 0.0)
        df_process.select('new_call_order_cnt', 'succ_rate', 'asp', 'gmv').show(10)

        from pyspark.sql.functions import udf,max
        from pyspark.sql.types import DoubleType
        df_temp = df_process.filter("(ori_c_rate== new_c_rate) and (ori_b_budget == new_b_budget) and (subsidy_money_lowerbound == cur_diaodu_subsidy)")
        ori_gmv=df_temp.groupBy(['city_id', 'pred_date']).agg(max("gmv").alias('gmv')).rdd.map(lambda x: ((x.city_id, x.pred_date), x.gmv)).collect()
        ori_gmv=dict(ori_gmv)

        print("*******ori_gmv********",ori_gmv)

        new_gmv_udf = udf(lambda cid, dt, gmv: gmv / (ori_gmv.get((cid, dt), 1.) + 1e-12) - 1., DoubleType())
        df_process=df_process.withColumn('dgmv', new_gmv_udf(df_process.city_id, df_process.pred_date, df_process.gmv))
        print("**********df_process.count()****************",df_process.count())
        return df_process 


    def extract_json_format(self, df):
        temp_df = df.query('pred_date == {}'.format(self.params['step_start_date'])).copy()  # 选择未来生效一天的数据
        temp_df = temp_df.astype({'treat_subsidy_hufan_rate_pk': 'float', 'treat_subsidy_hufan_rate_th': 'float',
                                    'gmv_k': 'float', 'gmv_th': 'float',
                                    'budget_k': 'float', 'budget_th': 'float','d_gmv_k': 'float', 'd_gmv_th': 'float'})

        
        reslist = []

        city_list = list(set(temp_df['city_id']))
        for city_id in city_list:

            city_df = temp_df.query('city_id == {}'.format(city_id))

            day_k_subsidy_dict = {
                "city_id":city_id,
                "product_line":"kuaiche",
                "caller":"c",
                "stat_date":self.params['step_start_date'],
                "stg_detail":[],
                "amount":city_df["budget_k"].iat[0],
                "gmv":city_df["gmv_k"].iat[0],
                "step_type":"c1"
            }
            day_th_subsidy_dict = {
                "city_id":city_id,
                "product_line":"tehui",
                "caller":"c",
                "stat_date":self.params['step_start_date'],
                "stg_detail":[],
                "amount":city_df["budget_th"].iat[0],
                "gmv":city_df["gmv_th"].iat[0],
                "step_type":"c1"
            }
            budget_k_all += day_k_subsidy_dict['amount']
            budget_th_all += day_th_subsidy_dict['amount']

            reslist.append(day_k_subsidy_dict)
            reslist.append(day_th_subsidy_dict)
    
        print('call back：')
        print('c k money: {}, c th money: {}'.format(budget_k_all, budget_th_all))
        return reslist
    def get_diaodu_lastweek_info(self, max_try_num = 6):
        for try_num in range(max_try_num):
            print('try_num', try_num)
            self.params['try_num'] = try_num
            print(lastweek_diaodu_sql.format(**self.params))
            lw_diaodu_info_temp = self.hc.sql(lastweek_diaodu_sql.format(**self.params))
            if (lw_diaodu_info_temp.count() > 0):
                lw_diaodu_info = lw_diaodu_info_temp.toPandas()
                return lw_diaodu_info
        if (self.params['try_num'] == max_try_num-1):
            self.error_callback('获取上周调度信息表数据失败!!!')
            
    def get_pred_and_alpha_num(self, max_try_num=30):
        # 得到城市预测信息和完单率alpha的信息。
        # 预测表是未来每天数据
        for try_num in range(max_try_num):
            self.params['pred_try_num'] = try_num
            print(pred_sql.format(**self.params))
            pred_dapan_info = self.hc.sql(pred_sql.format(**self.params))
            if(pred_dapan_info.count() == MIN_NUM_OF_DATA):
                continue
            st_end_times = sorted(self.params['st_end_times'])
            start_time, end_time = st_end_times[0], st_end_times[-1]
            pred_start_num = pred_dapan_info.filter(
                'pred_date == "{}"'.format(start_time)).count()
            pred_end_num = pred_dapan_info.filter(
                'pred_date == "{}"'.format(end_time)).count()
            if(pred_start_num > MIN_NUM_OF_DATA and pred_end_num > MIN_NUM_OF_DATA):
                break
        # 完单率参数固定参数
        for try_num in range(max_try_num):
            self.params['alpha_try_num'] = try_num
            pred_dapan_info = self.hc.sql(alpha_sql.format(**self.params))
            alpha_num = pred_dapan_info.count()
            if(alpha_num > MIN_NUM_OF_DATA):
                break
        print('pred_try_num:{}, alpha_try_num:{}'.format(
            self.params['pred_try_num'], self.params['alpha_try_num']))
        if(self.params['pred_try_num'] == max_try_num-1):
            self.error_callback('获取供需表数据失败!')
        if(self.params['alpha_try_num'] == max_try_num-1):
            self.error_callback('获取完单预估表数据失败 !')

    def get_b_elasticity(self, max_backup_num=7):
        # b端弹性，backup方案，往前找。
        for try_num in range(max_backup_num):
            self.params['b_elasticity_try_num'] = try_num
            b_elasticity_df = self.hc.sql(b_elasticity_sql.format(**self.params))
            b_elasticity_num = b_elasticity_df.count()
            print(b_elasticity_num)
            if(b_elasticity_num > MIN_NUM_OF_DATA):
                print('b_elasticity_num backup num: {} '.format(b_elasticity_num))
                return b_elasticity_df
        self.error_callback('B端弹性获取失败!')

    def get_bn_budget_vertify(self):
        bn_budget_vertify_url = self._api_url + "generalallocation/querystep"
            
        # 生效参数获取
        for item in json.loads(self.params['child_info']):
            if item['step_type'] == 'b_decision' :
                vertifystep_id = item['step_id']
                vertify_version = "d" if item['validation_version']== "" else item['validation_version']
        if vertifystep_id:
            bgt_params = {'order_id':self._stg_param['order_id'] ,
                    'step_id':vertifystep_id}
            bn_budget_version_list = callback_request(bn_budget_vertify_url,bgt_params)
            if bn_budget_version_list['errno'] == 0:
                bn_vertify_budget_list = bn_budget_version_list['data']['version_list']
                for bn_item in bn_vertify_budget_list:
                    if bn_item['version'] == vertify_version:
                        bn_vertify_budget = bn_item['data']
                        print(len(bn_item['data']))
            else:
                self.error_callback('获取B端周分配结果失败 !')
        else:
            self.error_callback('获取B端周分配结果失败 !')
        bn_vertify_budget_pd = pd.DataFrame(bn_vertify_budget).groupby(['city','stat_date']).agg({'amount':['sum']}).reset_index()
        print("successfully get bn_vertify_budget",bn_vertify_budget_pd.head(5))
        bn_vertify_budget_pd.columns = ['city_id', 'pred_date', 'ori_b_budget']
        self.hc.createDataFrame(bn_vertify_budget_pd).registerTempTable('bn_vertify_budget_pd')

        return bn_vertify_budget_pd  
    
    def generate_output_dd(self, choose_scheme):
        temp_df = choose_scheme.copy()
        temp_df = temp_df.astype({'total_gmv': 'float', 'ori_c_budget': 'float',
                                    'new_c_budget': 'float', 'ori_b_budget': 'float',
                                    'new_b_budget': 'float', 'new_c_budget_rate': 'float', 'new_b_budget_rate': 'float',
                                    'new_diaodu_budget_rate': 'float','cur_diaodu_subsidy': 'float',
                                    'ori_acc_budget':'float','ori_acc_rate':'float', 'subsidy_money_lowerbound':'float'})

        temp_df['t_gmv'] = temp_df['total_gmv'].apply(
            lambda x: 1.0 if round(x, 1)  == 0.0 else x )
        temp_df['cur_diaodu_subsidy'] = temp_df['cur_diaodu_subsidy'].apply(
            lambda x: 1.0 if round(x, 1) == 0.0 else x )

        reslist = []
        dis_ext_data_dict = {}
        for city_id in set(self.params['city_list'].split(",")):
            city_df = temp_df.query('city_id == {}'.format(city_id))
            cur_diaodu_subsidy = city_df['cur_diaodu_subsidy'].sum()
            lastweek_diaodu_subsidy = city_df['lastweek_diaodu_subsidy'].sum()
            lastweek_diaodu_gmv = city_df['lastweek_diaodu_gmv'].sum()
            diaodu_min_rate = city_df['subsidy_money_lowerbound'].sum()/city_df['t_gmv'].sum()
            if len(city_df) != 0 : 
                diaodu_ext_data_dict = {}
                diaodu_ext_data_dict['lastweek_subsidy'] = lastweek_diaodu_subsidy
                diaodu_ext_data_dict['last_gmv'] = lastweek_diaodu_gmv
                diaodu_ext_data_dict['min_rate'] = diaodu_min_rate*100 if cur_diaodu_subsidy != 0 else 0.0
                dis_ext_data_dict[city_id] = json.dumps(diaodu_ext_data_dict)
                for _,row in city_df.iterrows():
                    diaodu_json = {}
                    diaodu_json["stat_date"] = row.pred_date
                    diaodu_json["city"] = row.city_id
                    diaodu_json["product_line"] = "kuaiche"
                    diaodu_json["caller"] = "dispatch_b" 
                    diaodu_json["gmv"] = row.t_gmv
                    diaodu_json["step_type"] = "dispatch_allocation"
                    diaodu_json["stg_detail"] = []
                    diaodu_json["amount"] = row.cur_diaodu_subsidy
                    reslist.append(diaodu_json)
        version_info = []
        version_info_sub = {
            "rely_info" : {"gmv_dt": self.params['gmv_dt'],"cr" : "" },
            "version": "",
            "data":reslist,
            "ext_data": {"sum_cr_rate": {}, "delta_gmv": 0.0,"dis_ext_data":dis_ext_data_dict}
        }
        version_info.append(version_info_sub)
        resp = {
            "order_id": self._stg_param['order_id'],
            "step_id": self._stg_param['step_id'],
            "version_info":json.dumps(version_info)
        }
        return resp
    
    def generate_output_c(self, choose_scheme):
        temp_df = choose_scheme[choose_scheme['pred_date'] == self.params['step_start_date']]

        temp_df.drop_duplicates(subset=['city_id','pred_date','treat_subsidy_hufan_rate_pk','treat_subsidy_hufan_rate_th'],keep='first',inplace=True)
        print("T+1 预算总计:{} , 普快预算: {}, 特惠预算: {}".format(temp_df["budget_pk"].sum() + temp_df["budget_th"].sum(),temp_df["budget_pk"].sum(),temp_df["budget_th"].sum()))
        temp_df = temp_df.astype({'gmv_pk': 'float', 'gmv_th': 'float',
                                    'budget_k': 'float','budget_pk': 'float', 'budget_th': 'float',
                                    'treat_subsidy_hufan_rate_pk': 'float', 'treat_subsidy_hufan_rate_th': 'float'})

        temp_df['t_gmv_pk'] = temp_df['gmv_pk'].apply(
            lambda x: 1.0 if round(x, 1) == 0.0 else x )
        temp_df['t_gmv_th'] = temp_df['gmv_th'].apply(
            lambda x: 1.0 if round(x, 1) == 0.0 else x )

        # 对生效的t+1预算进行播报
        try:
            utils.c_broadcast(choose_scheme,temp_df,self._stg_param['is_online'],self.params['step_start_date'],self._stg_param['order_id'])
        except Exception:
            print("error while brodacast c budget info ")
        
        reslist = []
        for city_id in set(self.params['city_list'].split(",")):
            city_df = temp_df.query('city_id == {}'.format(city_id))
            for _,row in city_df.iterrows():
                c_k_json = {}
                c_th_json = {}
                c_k_json["stat_date"] = row.pred_date
                c_k_json["city"] = row.city_id
                c_k_json["product_line"] = "kuaiche"
                c_k_json["caller"] = "dape-aps"
                c_k_json["gmv"] = row.t_gmv_pk
                c_k_json["step_type"] = "hufan_T_1"
                c_k_json["stg_detail"] = []
                c_k_json["amount"] = row.budget_pk

                c_th_json["stat_date"] = row.pred_date
                c_th_json["city"] = row.city_id
                c_th_json["product_line"] = "tehui"
                c_th_json["caller"] = "dape-aps"
                c_th_json["gmv"] = row.t_gmv_th
                c_th_json["step_type"] = "hufan_T_1"
                c_th_json["stg_detail"] = []
                c_th_json["amount"] = row.budget_th

                reslist.append(c_k_json)
                reslist.append(c_th_json)
        version_info = []
        version_info_sub = {
        "rely_info" : {"gmv_dt": self.params['gmv_dt'],"cr" : "" },
            "version": "",
            "data":reslist,
            "ext_data": {"sum_cr_rate": {}, "delta_gmv": 0.0}
        }   
        version_info.append(version_info_sub)
        resp = {
            "order_id": self._stg_param['order_id'],
            "step_id": self._stg_param['step_id'],
            "version_info":json.dumps(version_info)
        }
        return resp
    
    def diaodu_index_df(self): #超参将try_num 的数值传过来，保证取到的日期能够吻合
        st_end_time = list(self.params['st_end_times'])
        lenth_st = [i for i in range(1,len(st_end_time)+1)]
        this_week_index = pd.DataFrame({
            'pred_date':st_end_time,
            'dt_idx':lenth_st
        })
        print('***********this week index********\n', this_week_index)

        s1 = datetime.datetime.strptime(self.params['start_time'], '%Y-%m-%d')
        s2 = datetime.datetime.strptime(self.params['end_time'], '%Y-%m-%d')
        diff_num = (s2-s1).days+1
        self.params['diff_num'] = diff_num
        cur_date = self.params['cur_day']
        cur_date = datetime.datetime.strptime(cur_date, "%Y-%m-%d")
        time_list = []
        for i in range(diff_num, 0, -1):
            last_info = (cur_date + datetime.timedelta(days= -i + self.params['try_num'])).strftime("%Y-%m-%d")
            time_list.append(last_info)
        print('***********last info**********\n', time_list)
        lenth_st_1 = [i for i in range(1, diff_num+1)]
        last_week_index = pd.DataFrame({
            'tdate' : time_list,
            'dt_idx' : lenth_st_1
        })
        print('***********last_week_index*******\n', last_week_index)
        return this_week_index, last_week_index
    
    def find_opti_dd_choose(self, opti_df, df_sample):
        # 1、类型转换
        choose_rows = opti_df.toPandas().reset_index(drop=True)
        choose_rows.city_id = choose_rows.city_id.astype(int)
        process_double_columns = ['total_gmv','ori_c_rate', 'new_c_rate', 'pred_dsend_ratio', 'ori_c_budget', 'budget_c_gap', 'ori_b_budget',
                              'ori_b_rate','ori_acc_budget','ori_acc_rate','new_b_budget', 'budget_b_gap', 'new_b_rate', 'base_online_time', 'asp', 'call_order_cnt',
                              'new_call_order_cnt', 'new_online_time', 'k1', 'k2', 'k3','gongxu_max','gongxu_min', 'diaodu_b_max','diaodu_b_min',
                              'gongxu','gongxu_normalization','cur_diaodu_normalization_rate', 'cur_diaodu_subsidy','subsidy_money_lowerbound',
                              'subsidy_money_upperbound','lastweek_avg_diaodu_rate', 'lastweek_diaodu_gmv', 'succ_rate','gmv', 'dgmv']
        for colname in process_double_columns:
            choose_rows[colname] = choose_rows[colname].astype(float)
        choose_rows['budget_b_gap'] = choose_rows.apply(lambda x: x.budget_b_gap if x.budget_b_gap >= 0 else 0.0,axis=1) #TODO check if wrong data from upside
        choose_rows['budget_c_gap'] = choose_rows.apply(lambda x: x.budget_c_gap if x.budget_c_gap >= 0 else 0.0,axis=1)
        # 剔除cur_diaodu_normalization_rate ==0 但是 cur_diaodu_subsidy 不为0 的
        choose_rows = choose_rows.drop( choose_rows[choose_rows['cur_diaodu_normalization_rate'] ==0 ][choose_rows['cur_diaodu_subsidy'] !=0].index )
        print('loading data from spark path , choose_rows:',choose_rows.head(5))
        
        # 2. 拿到b和c的填坑金额
        # 屏蔽的数据不统计填坑的钱
        choose_rows['ori_c_budget'] = choose_rows.apply(lambda x: x.ori_c_budget if x.new_c_rate != 0.0 else 0.0, axis=1)
        choose_rows['ori_b_budget'] = choose_rows.apply(lambda x: x.ori_b_budget if x.new_b_rate != 0.0 else 0.0, axis=1)
        choose_rows1 = choose_rows[['city_id','pred_date','ori_c_budget','ori_b_budget','ori_acc_budget']].drop_duplicates()
        print('size of choose row :', choose_rows1.shape)
        self.params['c_spent'] = choose_rows1.ori_c_budget.astype(float).sum()
        self.params['b_spent'] = choose_rows1.ori_b_budget.astype(float).sum() 
        self.params['acc_spent'] = choose_rows1.ori_acc_budget.astype(float).sum()
        print('统计填坑阶段已经花的钱：C端: {}, B端盘古: {}, 加速卡: {}'.format(self.params['c_spent'], self.params['b_spent'], self.params['acc_spent']))
        print("*************打印参数：***************",self.params)

        # 3.选择符合的调度
        choose_rows['subsidy_money_upperbound1'] = choose_rows.apply(lambda x: max(x.subsidy_money_upperbound, x.total_gmv * x.diaodu_b_max),axis=1)
        choose_rows = choose_rows.query('(cur_diaodu_subsidy < subsidy_money_upperbound1) or  \
                                        (cur_diaodu_subsidy == subsidy_money_lowerbound) ')
        
        # 4.数据的预处理
        opti_data = choose_rows.reset_index(drop=True)
        opti_data['idx'] = list(range(opti_data.shape[0]))
        opti_data['dt_gap'] = opti_data['cur_diaodu_subsidy'] - opti_data['subsidy_money_lowerbound']

        #如果出现调度屏蔽，会有cur_diaodu_sub = 0，但是diaodu lower bould 不为0的情况
        opti_data['subsidy_money_lowerbound'] = opti_data.apply(lambda x: 0.0 if x.dt_gap < 0 else x.subsidy_money_lowerbound, axis=1)
        opti_data['dt_gap'] = opti_data.apply(lambda x: 0.0 if x.dt_gap < 0 else x.dt_gap, axis=1)

        opti_data['total_cost'] = opti_data['budget_c_gap'] + opti_data['budget_b_gap']  # balance 阶段总包
        opti_data['dgmv'] = opti_data.apply(
            lambda x: x.dgmv if x.dgmv > 0 else 0.0, axis=1)
        # opti_data['dgmv'] = opti_data.apply(lambda x: x.dgmv if x.s_rate > 0.001 else 0.0, axis = 1)
        self.params['ori_total_budget'] = self.params['total_budget']
        dt_df = opti_data[['city_id','pred_date','subsidy_money_lowerbound']].drop_duplicates()
        self.params['dt_min_spent'] = dt_df.subsidy_money_lowerbound.sum()
        remain_budget = self.params['total_budget'] - self.params['b_spent'] - self.params['acc_spent'] - self.params['c_spent'] - self.params['dt_min_spent']
        print('总包金额：{}, 剩余金额: {}, B端已花: {}, 加速卡已花: {}, 调度已花：{}, C端已花：{}'.format(
            self.params['total_budget'], remain_budget, self.params['b_spent'],self.params['acc_spent'],self.params['dt_min_spent'], self.params['c_spent']))
        # 5.规划求解
        choose_scheme = utils.linear_contraint(opti_data, remain_budget, self.params)
        total_content = choose_scheme.copy()

        total_content.budget_b_gap = total_content.budget_b_gap.fillna(0.0)
        total_content.budget_c_gap = total_content.budget_c_gap.fillna(0.0)
        total_content.lastweek_diaodu_gmv = total_content.lastweek_diaodu_gmv.fillna(1e-14)
        total_content.gmv = total_content.gmv.fillna(0.0)
        total_content.succ_rate = total_content.succ_rate.fillna(0.0)
        total_content.ori_c_budget = total_content.ori_c_budget.fillna(0.0)
        total_content.ori_b_budget = total_content.ori_b_budget.fillna(0.0)
        total_content.type = total_content.type.fillna('none')
        total_content.k1 = total_content.k1.fillna(0.0)
        total_content.k2 = total_content.k2.fillna(0.0)
        total_content.k3 = total_content.k3.fillna(0.0)
        total_content.alpha = total_content.alpha.fillna(0.0)
        total_content.intercept = total_content.intercept.fillna(0.0)
        total_content.dgmv = total_content.dgmv.fillna(0.0)
        total_content.gongxu_normalization = total_content.gongxu_normalization.fillna(0.0)
        total_content.cur_diaodu_normalization_rate = total_content.cur_diaodu_normalization_rate.fillna(0.0)
        
        total_content['new_c_budget'] = total_content[['ori_c_budget', 'budget_c_gap']].apply(lambda x: x.ori_c_budget + x.budget_c_gap, axis=1)
        total_content['new_b_budget'] = total_content[['budget_b_gap', 'ori_b_budget']].apply(lambda x: x.budget_b_gap + x.ori_b_budget, axis=1) 
        total_content['cur_diaodu_subsidy'] = total_content.apply(lambda x: x.dt_gap + x.subsidy_money_lowerbound,axis=1)
        total_content['total_cost'] = total_content['budget_c_gap'] + total_content['budget_b_gap'] + total_content['cur_diaodu_subsidy']
        print_content = total_content.groupby('city_id').agg({'ori_c_budget': ['sum'],
                                                              'new_c_budget': ['sum'],
                                                              'ori_b_budget': ['sum'],
                                                              'new_b_budget': ['sum'],
                                                              'total_gmv': ['sum'],
                                                              'dgmv': ['mean']}).reset_index()
        print_content.columns = ['city_id', 'ori_c_budget', 'new_c_budget',
                                 'ori_b_budget', 'new_b_budget', 'total_gmv', 'dgmv']

        print_content['new_c_budget'] = print_content.apply(
            lambda x: x.new_c_budget if(x.new_c_budget/(x.total_gmv+1e-12) > 0.001) else 0.0, axis=1)
        print_content['new_b_budget'] = print_content.apply(
            lambda x: x.new_b_budget if(x.new_b_budget/(x.total_gmv+1e-12) > 0.001) else 0.0, axis=1)
        print("original budget c is {}, allocate budget c is {}, new budget c is {}".format(total_content['ori_c_budget'].sum(),total_content['budget_c_gap'].sum(),total_content['new_c_budget'].sum()))
        print("original budget b is {}, allocate budget b is {}, new budget b is {}".format(total_content['ori_b_budget'].sum(),total_content['budget_b_gap'].sum(),total_content['new_b_budget'].sum()))
        print("original budget d is {}, allocate budget d is {}, new budget d is {}".format(total_content['subsidy_money_lowerbound'].sum(),total_content['dt_gap'].sum(),total_content['cur_diaodu_subsidy'].sum()))
        # 分天

        print("params:",self.params)
        daily_optimal_result = operation_rule(self.params['total_budget'], total_content, self.params)
        
        # 20220818去除20%调度预算普惠逻辑
        """ 
        gmvdata = opti_data.query("cur_diaodu_subsidy > 0")[['city_id', 'pred_date','total_gmv']].drop_duplicates()
        totalgmv = np.sum(gmvdata['total_gmv'])
        diaodutotal = np.sum(daily_optimal_result['cur_diaodu_subsidy'])
        gmvdata['diaodu_def'] = gmvdata.apply(lambda x : x.total_gmv/totalgmv * 0.2 * diaodutotal , axis = 1)
        if 'total_gmv' in gmvdata:
            del gmvdata['total_gmv']
        my_daily_optimal_result = pd.merge(daily_optimal_result, gmvdata, how = 'left', on = ['city_id', 'pred_date'])
        my_daily_optimal_result['cur_diaodu_subsidy'] = my_daily_optimal_result.apply(lambda x : x.cur_diaodu_subsidy * 0.8 + x.diaodu_def, axis= 1)
        daily_optimal_result = my_daily_optimal_result.copy() 
        """

        # 聚合
 
        week_optimal_result = daily_optimal_result.groupby('city_id').agg({'pred_date': ['min'], 'ori_c_budget': ['sum'], 'new_c_budget': ['sum'], 
                                                                           'ori_b_budget': ['sum'], 'new_b_budget': ['sum'],'cur_diaodu_subsidy':['sum']
                                                                    ,'succ_rate':['mean'], 'total_gmv': ['sum'], 'dgmv': ['mean']}).reset_index()
        week_optimal_result.columns = ['city_id', 'pred_date', 'ori_c_budget','new_c_budget', 'ori_b_budget',
                                        'new_b_budget','cur_diaodu_subsidy','succ_rate', 'total_gmv', 'dgmv']
        week_optimal_result['new_c_budget_rate'] = week_optimal_result['new_c_budget'] / week_optimal_result['total_gmv']
        week_optimal_result['new_b_budget_rate'] = week_optimal_result['new_b_budget'] / week_optimal_result['total_gmv']
        week_optimal_result['new_diaodu_budget_rate'] = week_optimal_result['cur_diaodu_subsidy'] / week_optimal_result['total_gmv']

        week_optimal_result['roi'] = week_optimal_result.apply(lambda x: x.dgmv * x.total_gmv /(x.new_c_budget + x.new_b_budget + 1e-14),axis=1)
        print('周级别数据聚合：')
        print("week_optimal_result: ",week_optimal_result.head())
        print("####11#####temp_df.count",daily_optimal_result.shape[0])
        daily_optimal_result.drop('lastweek_diaodu_gmv', axis=1, inplace=True)

        # daily_optimal_result.to_csv("daily_optimal_result.csv")

        this_week_idx, last_week_idx = self.diaodu_index_df()
        lw_diaodu_info = self.get_diaodu_lastweek_info()
        lw_diaodu_info['lastweek_diaodu_subsidy'] = lw_diaodu_info['last_week_subsidy'].astype('float')
        lw_diaodu_info['lastweek_diaodu_gmv'] = lw_diaodu_info['lastweek_diaodu_gmv'].astype('float')
        print(lw_diaodu_info.head(100))
        lw_diaodu_info = lw_diaodu_info.merge(last_week_idx, how='left', on='tdate')
        daily_optimal_result = daily_optimal_result.merge(this_week_idx, how='left', on='pred_date')
        daily_optimal_result = pd.merge(daily_optimal_result, lw_diaodu_info, how='left', on=['city_id', 'dt_idx']) 
        print("####22#####temp_df.count",daily_optimal_result.shape[0])

        daily_optimal_result.drop(columns=['total_gmv'], inplace=True)
        total_gmv = self.hc.sql(gmv_sql.format(**self.params)).toPandas()
        daily_optimal_result = pd.merge(daily_optimal_result, total_gmv, how='left',left_on=['city_id', 'pred_date'], right_on=['city_id', 'estimate_date'])
        daily_optimal_result['subsidy_money_lowerbound'] = 0.0005*daily_optimal_result['total_gmv'].astype('float')
        daily_optimal_result['gap'] = daily_optimal_result['cur_diaodu_subsidy'] - daily_optimal_result['subsidy_money_lowerbound']
        neg = daily_optimal_result[daily_optimal_result['gap'] < 0]['gap'].sum()
        pos = daily_optimal_result[daily_optimal_result['gap'] >= 0]['gap'].sum()
        scala = (pos + neg)/pos
        daily_optimal_result['cur_diaodu_subsidy'][daily_optimal_result['gap'] >= 0] = daily_optimal_result['gap']*scala + daily_optimal_result['subsidy_money_lowerbound']
        daily_optimal_result['cur_diaodu_subsidy'][daily_optimal_result['gap'] < 0] = daily_optimal_result['subsidy_money_lowerbound']
        daily_optimal_result.drop(columns=['gap'], inplace=True)

        return daily_optimal_result


    def error_callback(self, errmsg ):
        utils.send_message(self._stg_param['title'],self._stg_param['is_online'], errmsg )
        error_msg = {
            "err_no": 1,
            "err_msg": errmsg,
            "order_id": self._stg_param['order_id'],
            "step_id": self._stg_param['step_id']
        }
        stg_api_url = self._api_url + "generalallocation/stgcallback"
        print("callback stg url: ", stg_api_url)
        # print(stg_results)
        resp = callback_request(stg_api_url, error_msg)
        print('callback response: %s' % resp)
        raise Exception(errmsg)

    def get_df_sample(self, ext_data):
        temp_list = []
        if len(json.loads(ext_data)['input_city_info']) == 0 :
            self.error_callback("输入城市为空 ")
        for line in json.loads(ext_data)['input_city_info']:
            date_list = []
            caller_list = []
            date_list.append(line['d'])
            caller_list.append(line['ca'])
            print(date_list, caller_list, line['cis'])
            date_city_index = pd.MultiIndex.from_product([ date_list,  caller_list, line['cis']], names=["stat_date", "tool","city_id"])
            temp_line = pd.DataFrame(index = date_city_index).reset_index()
            temp_line['pred_date'] = temp_line['stat_date']
            temp_list.append(temp_line)
        return temp_list

    def get_subsidy_rate_dict(self):
        print(get_subsidy_rate_sql.format(**self.params))
        get_subsidy_rate_df = self.hc.sql(get_subsidy_rate_sql.format(**self.params)).toPandas()
        subsidy_rate_dict = json.loads(get_subsidy_rate_df.at[0, "multi_sr"].replace("\\", ""))
        if len(subsidy_rate_dict.keys()) == 0:
            self.error_callback('C端T+1分配-核销补贴率字典为空！')
        # 生成补贴率字典
        th_pk_dd = {}
        keys = subsidy_rate_dict.keys()
        for pkhf in sorted(keys):
            tehuilist = sorted(subsidy_rate_dict[pkhf])
            pkhf = float(pkhf)
            for thhf in tehuilist:
                thhf = float(thhf)
                if (thhf * 1000) % 5 == 0:
                    if thhf not in th_pk_dd:
                        th_pk_dd[thhf] = pkhf
                    else:
                        th_pk_dd[thhf] = min(pkhf, th_pk_dd[thhf])
        return th_pk_dd

    def gen_allocation_fkrate(self, opti_data, c_budget):
        opti_data = opti_data.query("pred_date >= '%s'"% self.params['step_start_date'])
        gmvdf = opti_data.drop_duplicates(subset=['city_id', 'pred_date', 'gmv_pk', 'gmv_th'], keep='first')
        if len(gmvdf) == 0:
            self.error_callback('C端T+1分配-候选集去重后为空集！')
        gmvdf['gmv'] = gmvdf['gmv_pk'] + gmvdf['gmv_th']
        gmv_sum = gmvdf['gmv'].sum()
        if gmv_sum == 0:
            self.error_callback('C端T+1分配-预估gmv之和为0！')
        cur_fkrate = c_budget / gmv_sum
        print("分配期间：平均泛快补贴率：", cur_fkrate)
        return cur_fkrate


    def scale_min_budget_choose_scheme(self, filter_opti_df, budget_agg_df, min_budget, c_budget):
        # 最小预算可行解
        if min_budget > 0:
            scale_ratio = float(c_budget / min_budget)
            print("所需最小预算：", min_budget)
            print("scale ratio: ", scale_ratio)
        else:
            self.error_callback("呼返T+1: 输入的预算不大于0！")
        # 最小预算的可行解
        choose_scheme = pd.merge(filter_opti_df, budget_agg_df, on=['city_id', 'pred_date', 'budget'], how='inner')
        # scale_cols = [['treat_subsidy_hufan_rate_pk', 'treat_subsidy_hufan_rate_th', 'budget_k', 'budget_pk', 'budget_th']]
        # for coln in scale_cols:
        #     choose_scheme[coln] = choose_scheme[coln] * scale_ratio
        #     print(choose_scheme.head())
        cur_costs = choose_scheme['budget_pk'].sum() + choose_scheme['budget_th'].sum()
        gap = c_budget - cur_costs
        if (gap != 0):
            cur_gap_costs = choose_scheme['budget_pk'].sum() + choose_scheme['budget_th'].sum()
            choose_scheme['budget_pk'] = choose_scheme['budget_pk'].apply(lambda x: x + x * gap / cur_gap_costs)
            choose_scheme['budget_th'] = choose_scheme['budget_th'].apply(lambda x: x + x * gap / cur_gap_costs)
            choose_scheme['treat_subsidy_hufan_rate_th'] = choose_scheme['budget_th'] / choose_scheme['gmv_th']
            choose_scheme['treat_subsidy_hufan_rate_pk'] = choose_scheme['budget_pk'] / choose_scheme['gmv_pk']
        cur_costs = choose_scheme['budget_pk'].sum() + choose_scheme['budget_th'].sum()
        gap = c_budget - cur_costs
        print('放缩后：总预算是 {} ,已规划预算是{}, 普快预算{}, 特惠预算{} , 规划和预算的gap是{} :'.format(c_budget, cur_costs, choose_scheme['budget_pk'].sum(), choose_scheme['budget_th'].sum(), gap))
        return choose_scheme

        

    def b_budget_allocation(self):
        """
        B端T+N分配流程
        """
        # 1.得到输入参数

        cur_day, trace_id, rely_info, start_date, end_date, step_start_date, step_end_date, \
            total_budget, cost,  version_list, child_info, ext_data, gap_rate, accelerate_card_rate_dict,\
            pangu_rate_dict,step_id,order_id,step_type = self.get_global_infos()
        
        pred_dates = list(pd.date_range(start_date, end_date).astype('str'))
        if len(pred_dates) < 1:
            self.error_callback('参数错误，分配时间不足一天!')
        
        # 2.获取待分配城市信息
        df_sample_list = self.get_df_sample(ext_data)
        df_sample= pd.concat(df_sample_list).replace('accelerate_card','加速卡').replace('b','智能盘古')
                
        if 'cr_dt' in json.loads(rely_info).keys():
            cr_dt = json.loads(rely_info)['cr_dt']
            cr_type = json.loads(rely_info)['cr_type']
        else:
            cr_dt = cur_day
            cr_type = "objective_exp_openapi_pp"

        self.params = {
            'city_list': ",".join(map(str, list(set(df_sample['city_id'])))),
            'cur_day': cur_day,
            'start_time': start_date,
            'end_time': end_date,
            'trace_id': trace_id,
            'total_budget':total_budget,
            'b_total_budget': gap_rate * total_budget,
            'version_list':version_list,
            'accelerate_card_rate_dict': accelerate_card_rate_dict,
            'pangu_rate_dict':pangu_rate_dict,
            'st_end_times' : pred_dates,
            'st_end_times_str': str(pred_dates)[1:-1],
            'gmv_dt':json.loads(rely_info)['gmv_dt'],
            'cr_dt':cr_dt,
            'cr_type':cr_type,
            'step_id':step_id,
            'order_id':order_id,
            'step_type':step_type
        }

        print('B端填坑总预算:',self.params['b_total_budget'])
        print("打印参数:",self.params)

        # 3 T+N 分配主流程
        # 3.1 获取供需诊断数据   
        gap_dignosis_all_strategy, fixed_threshold_list,allocate_version_list= self.get_pregap_dignosis_info(df_sample)

        # 3.2 获取弹性数据 
        pangu_elastic, acc_elastic = self.get_elastic_b_hourly(df_sample)
        
        # 3.3 获取预测的完单和呼返、城市天的asp数据
        forecast_daily_data = self.get_forecast_data_for_b_fillup(df_sample)
        asp_df = self.get_asp_df()        
        
        # 4 对不同的诊断数据进行分别规划求解
        b_budget_list = []
        for i in range(len(fixed_threshold_list)):
            gap_dignosis = gap_dignosis_all_strategy[gap_dignosis_all_strategy['strategy_type'] == fixed_threshold_list[i]].drop('strategy_type',1)
            gap_b_table_df = self.get_b_fillup_optimal_data(gap_dignosis, acc_elastic, pangu_elastic, forecast_daily_data, asp_df,allocate_version_list[i])
            # 结果处理
            b_budget_dict = self.extract_json_format_b_budget(gap_b_table_df,fixed_threshold_list[i],forecast_daily_data)
            b_budget_list.append(b_budget_dict)
    
        # 5 返回结果

        b_fillup_ans = {
            "order_id":self.params['order_id'],
            "step_id": self.params['step_id'],
            "version_info": json.dumps(b_budget_list)
        }
        print("finish calculate the result !! ")

        return b_fillup_ans


    def dd_budget_allocation(self):
        # 1.获取主要参数
        cur_day, trace_id, rely_info, start_date, end_date, step_start_date, step_end_date, total_budget, \
            cost,  version_list, child_info, ext_data, gap_rate,accelerate_card_rate_dict, pangu_rate_dict,step_id,order_id,step_type = self.get_global_infos()

        pred_dates = list(pd.date_range(start_date, end_date).astype('str'))
        if len(pred_dates) < 1:
            self.error_callback('参数错误，分配时间不足一天!')

        df_sample_list = self.get_df_sample(ext_data)
        df_sample= pd.concat(df_sample_list)

        self.hc.createDataFrame(df_sample).registerTempTable("df_sample")
        city_list = list(set(df_sample.query("tool == 'dispatch_b' ").city_id))

        self.params = {
            'city_list': ",".join(map(str, city_list)),
            'cur_day': cur_day,
            'gmv_dt':json.loads(rely_info)['gmv_dt'],
            'start_time': start_date,
            'end_time': end_date,
            'trace_id': trace_id,
            'step_start_date':step_start_date,
            'step_end_date':step_end_date,
            'total_budget':total_budget,
            'cost':cost,
            'b_total_budget': gap_rate * total_budget,
            'version_list':version_list,
            'child_info':child_info,
            'accelerate_card_rate_dict': accelerate_card_rate_dict,
            'pangu_rate_dict':pangu_rate_dict,
            'st_end_times':pred_dates,
            'st_end_times_str':str(pred_dates)[1:-1]
        }
        print("打印参数：",self.params)

        # 2.获取C端弹性数据，B端预算数据，调度上下界数据和供需、计算 dgmv
        self.get_pred_and_alpha_num()
        self.params['pred_sql'] = pred_sql.format(**self.params)
        self.params['alpha_sql'] = alpha_sql.format(**self.params)

        opti_df = self.merge_all_pre()

        # 3.整数规划
        choose_scheme = self.find_opti_dd_choose(opti_df, df_sample)
        print("choose_scheme:")
        print(choose_scheme.head(20))
    
        # 4.结果处理
        res_json_dd = self.generate_output_dd(choose_scheme)
        
        return res_json_dd



    def c_budget_allocation(self):

        # 1.获取主要参数
        print("raw—param",self._stg_param)
        cur_day, trace_id, rely_info, start_date, end_date, step_start_date, step_end_date, total_budget, \
            cost,  version_list, child_info, ext_data, gap_rate,accelerate_card_rate_dict, pangu_rate_dict,step_id,order_id,step_type = self.get_global_infos()

        gross_profit_fee = json.loads(ext_data)['gross_profit_fee']

        pred_dates = list(pd.date_range(start_date, end_date).astype('str'))
        if len(pred_dates) < 1:
            self.error_callback('参数错误，分配时间不足一天!')
        
        df_sample_list = self.get_df_sample(ext_data)
        df_sample= pd.concat(df_sample_list)


        city_list = list(set(df_sample.query(" tool == 'dape-aps' ").city_id)) # 获取city_list
        pred_dates_temp = pred_dates.copy()
        # try:
        #     pred_dates_temp.remove(step_start_date)
        # except:
        #     pred_dates_temp
        date_city_index = pd.MultiIndex.from_product([pred_dates_temp, city_list], names=["pred_date", "city_id"])
        df_sample_res = self.hc.createDataFrame(pd.DataFrame(index=date_city_index).reset_index()).toPandas()
        df_sample = pd.concat([df_sample[['pred_date','city_id']],df_sample_res],axis = 0)
        self.hc.createDataFrame(df_sample).registerTempTable("df_sample")

        self.params = {
            'city_list_l':city_list,
            'city_list': ",".join(map(str, city_list)),
            'cur_day': cur_day,
            'gmv_dt':json.loads(rely_info)['gmv_dt'],
            'start_time': start_date,
            'end_time': end_date,
            'trace_id': trace_id,
            'step_start_date':step_start_date,
            'step_end_date':step_end_date,
            'total_budget':total_budget,
            'cost':cost,
            'b_total_budget': gap_rate * total_budget,
            'gross_profit_fee': gross_profit_fee,
            'version_list':version_list,
            'child_info':child_info,
            'accelerate_card_rate_dict': accelerate_card_rate_dict,
            'pangu_rate_dict':pangu_rate_dict,
            'st_end_times':pred_dates,
            'st_end_times_str':str(pred_dates)[1:-1]
        }
        print("打印参数", self.params)

        # 2.根据毛利额计算C端剩余预算 C端预算 = [tr_pk + tr_th]+ {tr_pk + tr_th}- [cost[B端预算 & C端已花预算 & T+N 调度)] - {B 端预算} - [ykj_sub] - {ykj_sub} - gross_profit_total
        # # 2.1 获取 [tr_pk + tr_th]+ {tr_pk + tr_th} - [ykj_sub] - {ykj_sub}

        tr_th_amount_sum, tr_k_amount_sum, ykj_amount_sum = self.get_raw_gf_data()
        
        # 2.2 gmv_tr_ykj_info - [cost[B端预算 & C端已花预算 & T+N 调度)] - {B 端预算} - gross_profit_total
        """
        c_budget_temp = tr_th_amount_sum + tr_k_amount_sum - ykj_amount_sum
        bn_budget_vertify = self.get_bn_budget_vertify()
        b_budget_pre = bn_budget_vertify[bn_budget_vertify['pred_date'] > self.params['step_start_date']]['ori_b_budget'].sum() # 获取未来的B端预算
        c_budget = c_budget_temp - b_budget_pre - cost - gross_profit_fee 
        """
        # 0510 FIXME 新增呼返剩余预算，暂不根据毛利转换 
        c_budget = self._stg_param['budget_limit']
        print("succesefully calculate the rest budget for C ", c_budget)
        
        # 1.获取后验roi
        roidf = self.get_roi_df()
        print('roidf count:', len(roidf))

        # 2.生成核销字典
        hexiao_ratedict = self.get_subsidy_rate_dict()
        print("补贴率字典：", hexiao_ratedict)
        print("补贴率字典key", hexiao_ratedict.keys())

        # 3.获取C端弹性和供需数据、计算 dgmv
        c_elestic_k_th_df = self.get_c_elasticity()
        print('c elasticity count:', c_elestic_k_th_df.count())

        opti_df = self.get_opti_data(c_elestic_k_th_df)
        # 兜底操作：候选集去重
        opti_df = opti_df.drop_duplicates(subset=['city_id','pred_date','treat_subsidy_hufan_rate_pk','treat_subsidy_hufan_rate_th'],keep='first')
        print('opti_df count:', len(opti_df))

        # 3. 读取对照组补贴率
        ctl_kuaidf, ctl_tehuidf = self.get_ctl_estimate_rate(c_budget, opti_df)
        print('ctl_kuaidf count:', len(ctl_kuaidf), 'ctl_tehui count:', len(ctl_tehuidf))

        # 4.获取gmv top 50城市
        topcitystr = gen_top_cities(opti_df)
        print("top 50 city:", topcitystr)

        # 5.根据规则过滤数据
        filter_opti_df = self.pid_filter_opti_data(opti_df, roidf, ctl_kuaidf, ctl_tehuidf, hexiao_ratedict, topcitystr)
        print('filter_opti_df count:', len(filter_opti_df))

        # 计算所需的最小预算
        filter_opti_df['budget']=filter_opti_df['budget_pk'] + filter_opti_df['budget_th']
        budget_agg_df = filter_opti_df.groupby(['city_id', 'pred_date'])[['budget']].agg('min').reset_index()
        min_budget = budget_agg_df['budget'].sum()
        if min_budget > c_budget:
            print("最小预算的可行解预算大于总预算，放缩最小预算的可行解！！")
            choose_scheme = self.scale_min_budget_choose_scheme(filter_opti_df, budget_agg_df, min_budget, c_budget)
        else:
            # 6.整数规划
            choose_scheme = self.find_opti_c_choose(filter_opti_df, c_budget)
        print(choose_scheme.head())

        # 7.结果处理
        res_json_c = self.generate_output_c(choose_scheme)

        return res_json_c



