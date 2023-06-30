#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import sys
import logging
import traceback
import requests
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
from utils import load_conf, contraint_opt, use_day_contraint_opt, linear_contraint, operation_rule, callback_request,upload_to_gift,send_message
from hive_sql import *
from functools import reduce
import subprocess
import warnings
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

    def get_global_infos(self):
        '''
        get all the input and param we need
        :return:
        '''

        xman_city_list = self._stg_param['city_list']
        # zhanqu_city_lists = self._config['zhanqu_city_lists']
        if(len(self._stg_param['zq_city']) != 0):
            self._config['zhanqu_city_lists'] = self._stg_param['zq_city']
        zhanqu_city_lists = self._config['zhanqu_city_lists']
        holiday_set = self._config['holiday_set']
        tengnuo_city_lists = self._config['tengnuo_city_lists']
        # 去掉战区城市。
        city_list = list(set(xman_city_list))
        # city_list = list(set(xman_city_list) & set(tengnuo_city_lists))
        # city_list = list(set(xman_city_list) & set(tengnuo_city_lists) - set(zhanqu_city_lists))
        # cur_day = self._stg_param['cur_day']
        cur_day = self._stg_param['trigger_time'].split(' ')[0]
        start_date = self._stg_param['start_date']
        end_date = self._stg_param['end_date']
        trace_id = self._stg_param['trace_id']
        total_budget = self._stg_param['budget']/MONEY_UNIT
        remove_date = self._stg_param['remove_date']

        # todo 1、通过接口调用，获取填坑预算比例和填坑加速卡补贴率率的离散值
        apollo_url = "http://10.88.128.149:32234/darwin/config/getapollo"
        # B端填坑比例参数传递
        ratio_params = {'namespace':'dape_darwin_cfg',
                'cfg_name':'gap_balance_rate_cfg',
                'stg_name':'all'}
        budget_allocate_ratio = callback_request(apollo_url,ratio_params)
        gap_rate_res = json.loads(budget_allocate_ratio["data"]["gap_rate"])
        ballance_rate_res = json.loads(budget_allocate_ratio["data"]["balance_rate"])
        if gap_rate_res is not None and ballance_rate_res is not None:
            gap_rate = gap_rate_res["total"]/(gap_rate_res["total"] + ballance_rate_res["total"])
        else:
            gap_rate = GAP_RATE_DEFAULT

        # 盘古｜加速卡补贴率参数传递
        rate_params = {'namespace':'dape_darwin_cfg',
                'cfg_name':'b_rate_selection_list',
                'stg_name':'all'}
        b_gap_city_rate = callback_request(apollo_url,rate_params)
        accelerate_card_rate_dict = json.loads(b_gap_city_rate["data"]["accelerate_card"])
        pangu_rate_dict = json.loads(b_gap_city_rate["data"]["pangu"])

        return city_list, cur_day, start_date, end_date, trace_id, total_budget, holiday_set, remove_date, gap_rate, accelerate_card_rate_dict, pangu_rate_dict

    def label_bk_city_day_udf(self, bk_day_tool_dict):
        '''
        pyspark udf实现，对屏蔽的城市添加标签
        :param json_acc_dict:
        :return:
        '''

        def label_bk_city_day(pred_date,city_id,new_b_rate,ori_acc_rate,new_c_rate,cur_diaodu_subsidy):
            bk_label = 0
            city_day_idx = pred_date +"_"+ str(city_id)

            if city_day_idx in bk_day_tool_dict.keys():
                bk_caller = bk_day_tool_dict[city_day_idx]
                lab_lst = []
                for i_caller in bk_caller:
                    if i_caller == 'b':
                        lable_temp = 1 if float(new_b_rate) != 0.0 else 0
                    if i_caller == 'accelerate_card':
                        lable_temp = 1 if float(ori_acc_rate) != 0.0 else 0
                    if i_caller == 'dape-aps':
                        lable_temp = 1 if float(new_c_rate) != 0.0 else 0
                    if i_caller == 'dispatch_b':
                        lable_temp = 1 if float(cur_diaodu_subsidy) != 0.0 else 0
                    lab_lst.append(lable_temp)
                bk_label = max(lab_lst)
                print("bk_label:",bk_label)
            return bk_label
            
        return F.udf(label_bk_city_day, IntegerType())

    def generate_backlist_dict(self):
        """
        将屏蔽的信息转换成城市天粒度
        { "2022-01-01_1": [b,accelerate_card,...],
        ...
        }
        """
        for date_elem in self._stg_param['blackdict']:
            if date_elem['date'] in self._stg_param['remove_date']:
                self._stg_param['blackdict'].remove(date_elem)
        for dt in self._stg_param['remove_date']:
            temp_dict = {'date':dt,
                            'black_list':[{"city":list(map(int,self.params['city_list'].split(','))),
                                        "caller":["dape-aps","b","dispatch_b","accelerate_card"]}]
                        }
            self._stg_param['blackdict'].append(temp_dict)

        print("the blackdict in params",self._stg_param['blackdict'])
        bk_day_tool_dict = {}
        for day_bk_iterm in  self._stg_param['blackdict']:
            bk_date = day_bk_iterm['date']
            bk_list = day_bk_iterm['black_list']
            for iter in bk_list:
                for iter_city in iter['city']:
                    city_day_idx = bk_date +"_"+ str(iter_city)
                    for iter_caller in iter['caller']:
                        if city_day_idx in bk_day_tool_dict.keys():
                            bk_day_tool_dict[city_day_idx].append(iter_caller)
                        else:
                            bk_day_tool_dict[city_day_idx] = [iter_caller]
        print("汇总城市天粒度屏蔽的dict：",bk_day_tool_dict)
        return bk_day_tool_dict

    def generate_backlist_str(self):
        for date_elem in self._stg_param['blackdict']:
            if date_elem['date'] in self._stg_param['remove_date']:
                self._stg_param['blackdict'].remove(date_elem)
        for dt in self._stg_param['remove_date']:
            temp_dict = {'date':dt,
                         'black_list':[{"city":list(map(int,self.params['city_list'].split(','))),
                                       "caller":["dape-aps","b","dispatch_b","accelerate_card"]}]
                        }
            self._stg_param['blackdict'].append(temp_dict)

        print("the blackdict in params",self._stg_param['blackdict'])
        total_backlist = []
        black_date_list = []
        for day_black_dict in self._stg_param['blackdict']:
            bk_time = day_black_dict['date']
            black_date_list.append("'{}'".format(bk_time))
            blacklist = day_black_dict['black_list']
            temp_date_str = "(pred_date == '{}')".format(bk_time)
            black_city_list = []
            for city_strategy_select_info in blacklist:
                city_id = ','.join(map(str,city_strategy_select_info['city']))
                black_city_list.extend(city_strategy_select_info['city'])
                temp_date_city_str = temp_date_str + " and (int(city_id) in ({})) ".format(city_id)
                if 'dape-aps' in city_strategy_select_info['caller']:
                    temp_date_city_str +=  " and (float(new_c_rate) == 0.0) "
                else:
                    temp_date_city_str +=  " and (float(new_c_rate) >= float(ori_c_rate)) "
                if 'b' in city_strategy_select_info['caller']:                   
                    temp_date_city_str +=  " and (float(new_b_rate) == 0.0) "
                else:
                    temp_date_city_str +=  " and (float(new_b_rate) >= float(ori_b_rate)) "
                if 'dispatch_b' in city_strategy_select_info['caller']:
                    temp_date_city_str +=  " and (float(cur_diaodu_subsidy) == 0.0) " 
                else:
                    temp_date_city_str +=  " and (float(cur_diaodu_subsidy) >= float(subsidy_money_lowerbound)) " 
                if 'accelerate_card' in city_strategy_select_info['caller']:
                    temp_date_city_str +=  " and (float(ori_acc_rate) == 0.0) "                       
                total_backlist.append('('+ temp_date_city_str +')')
            print("black_city_list",black_city_list)
            bk_city_str = ','.join(map(str,black_city_list))
            temp_str = " ( (pred_date == '{}') \
                        and (int(city_id) not in ({})) \
                        and (float(new_c_rate) >= float(ori_c_rate)) \
                        and (float(new_b_rate) >= float(ori_b_rate)) \
                        and (float(cur_diaodu_subsidy) >= float(subsidy_money_lowerbound)) \
                        ) ".format(bk_time,bk_city_str)
            total_backlist.append(temp_str)
        if len(black_date_list) != 0:
            print("black_date_list",black_date_list)
            total_backlist.append(' ( (pred_date not in ({})) \
                                    and (float(new_c_rate) >= float(ori_c_rate)) \
                                    and (float(new_b_rate) >= float(ori_b_rate)) \
                                    and (float(cur_diaodu_subsidy) >= float(subsidy_money_lowerbound)) \
                                    ) '.format(','.join(black_date_list)))
        else:
             total_backlist.append(' (pred_date is not null)  \
             and (float(new_c_rate) >= float(ori_c_rate)) \
                                    and (float(new_b_rate) >= float(ori_b_rate)) \
                                    and (float(cur_diaodu_subsidy) >= float(subsidy_money_lowerbound))' )
        print("******** total_black_str**********",' or '.join(total_backlist))
        return ' or '.join(total_backlist)
        
    def get_c_fillup_month_data(self, max_try_num=7):
        # c端填坑表默认是14天，要扩充成月级别的数据，对第14天的结果复制多份。
        st_end_times = sorted(self.params['st_end_times'])
        start_time, end_time = st_end_times[0], st_end_times[-1]
        for try_num in range(max_try_num):
            self.params['c_fillup_try_num'] = try_num
            # 默认c端只产出明天到下周日14天数据。
            c_fillup_df = self.hc.sql(c_fillup_sql.format(**self.params))
            if(c_fillup_df.count() == MIN_NUM_OF_DATA):
                continue # check
            max_date = max(c_fillup_df.toPandas()['pred_date'])
            max_date_df = c_fillup_df.filter(
                'pred_date == "{}"'.format(max_date))

            month_level_fillup_list = [c_fillup_df]
            for i in range(1, num_of_expand_days, 1):
                df_elem = max_date_df.withColumn('pred_date', date_add(col('pred_date'), i))\
                    .withColumn('pred_date', col('pred_date').astype("string"))
                month_level_fillup_list.append(df_elem)
            month_level_fillup_df = reduce(
                DataFrame.unionAll, month_level_fillup_list)
            pred_start_num = month_level_fillup_df.filter(
                'pred_date == "{}"'.format(start_time)).count()
            pred_end_num = month_level_fillup_df.filter(
                'pred_date == "{}"'.format(end_time)).count()
            print(pred_start_num, pred_end_num)
            if(pred_start_num > 1 and pred_end_num > 1):
                return month_level_fillup_df
        print('pred_try_num:{}'.format(self.params['c_fillup_try_num']))
        if(self.params['c_fillup_try_num'] == max_try_num-1):
            raise Exception('get month_level_fillup_list fail !')

    def get_b_fillup_month_data_old(self, max_try_num=7):
        st_end_times = sorted(self.params['st_end_times'])
        start_time, end_time = st_end_times[0], st_end_times[-1]
        for try_num in range(max_try_num):
            self.params['b_fillup_try_num'] = try_num
            # 默认b端只产出下周一到下周日7天数据。
            b_table = self.hc.sql(b_fillup_sql.format(**self.params)).cache()
            if(b_table.count() == MIN_NUM_OF_DATA):
                continue
            b_table_info = b_table.select([F.max(col('pred_date')).alias(
                'max'), F.min(col('pred_date')).alias('min')]).collect()[0]
            print('b_table max_date: {}, min_date:{}'.format(
                b_table_info.max, b_table_info.min))
            rdds = []
            for i in range(-1, 3, 1):
                b_table_tmp = b_table.select(['city_id', date_add(
                    col('pred_date'), 7*i).alias('pred_date'), 'ori_b_budget'])
                rdds.append(b_table_tmp)
            b_table = reduce(DataFrame.unionAll, rdds)
            b_table = b_table.filter(
                'pred_date in ({st_end_times_str})'.format(**self.params))
            print('b_table, len: ', b_table.count())
            self.params['b_spent'] = b_table.agg({'ori_b_budget': 'sum'}).collect()[
                0]['sum(ori_b_budget)']
            pred_start_num = b_table.filter(
                'pred_date == "{}"'.format(start_time)).count()
            pred_end_num = b_table.filter(
                'pred_date == "{}"'.format(end_time)).count()
            print(pred_start_num, pred_end_num)
            if(pred_start_num > 1 and pred_end_num > 1):
                return b_table
        print('pred_try_num:{}'.format(self.params['b_fillup_try_num']))
        if(self.params['b_fillup_try_num'] == max_try_num-1):
            raise Exception('get b_table fail !')


    def return_b_gap_info(self, choose_scheme):
        res_df = pd.DataFrame()
        for city in set(choose_scheme['city_id']):
            city_df = choose_scheme.query("city_id == %d"%(city))
            for day in set(city_df['stat_date']):
                temp = {}
                city_day_df = city_df.query("stat_date == '%s'"% (day))
                ori_b_budget = 0.0
                ori_acc_budget = 0.0
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
                temp['city_id'] = city
                temp['pred_date'] = day
                temp['ori_b_budget'] = round(ori_b_budget,3)
                temp['ori_acc_budget'] = round(ori_acc_budget,3)
                temp['pangu_stg_detail'] = json.dumps(pangu_stg_detail)
                temp['acc_card_stg_detail'] = json.dumps(acc_card_stg_detail)
                res_df = res_df.append(pd.DataFrame([temp]))
        return res_df

    def generate_gap_bk_list(self):
        """
        根据 black dict 生成 '城市_天_tool' 城市屏蔽索引list
        """
        for date_elem in self._stg_param['blackdict']:
            if date_elem['date'] in self._stg_param['remove_date']:
                self._stg_param['blackdict'].remove(date_elem)
        for dt in self._stg_param['remove_date']:
            temp_dict = {'date':dt,
                            'black_list':[{"city":list(map(int, self.params['city_list'].split(','))),
                                        "caller":["dape-aps","b","dispatch_b"]}]
                        }
            self._stg_param['blackdict'].append(temp_dict)

        bk_day_tool_list = []
        for day_bk_iterm in  self._stg_param['blackdict']:
            bk_date = day_bk_iterm['date']
            bk_list = day_bk_iterm['black_list']
            for iter in bk_list:
                for cal_iter in iter['caller']:
                    if cal_iter == 'b':
                        for cid in iter['city']:
                            cid_idx = str(cid)+"_"+bk_date+"_"+"智能盘古"
                            bk_day_tool_list.append(cid_idx)
                    elif cal_iter == 'accelerate_card':
                        for cid in iter['city']:
                            cid_idx = str(cid)+"_"+bk_date+"_"+"加速卡"
                            bk_day_tool_list.append(cid_idx)
                    else:
                        continue
        bk_day_tool_list = list(set(bk_day_tool_list))
        return  bk_day_tool_list

    def agg_event_2_city_options(self,x):
        '''
        聚合到城市维度，并将活动信息转换成dict存储在新的字段里
        :return:
        '''
        pangu_stg_detail = ""
        acc_card_stg_detail = ""
        event_dict = {
            "fence_id": x.fence_id,
            "time_slot_st": x.start_time,
            "time_slot_et": x.end_time,
            "amount":x.subsidy,
            "param":json.dumps({"cr": x.cr,
                     "cr_bar": x.cr_bar,
                     "gmv": x.gmv})
        }
        if x.tool == "智能盘古":
            pangu_stg_detail = event_dict
        elif x.tool == "加速卡" :
            acc_card_stg_detail = event_dict

        return pangu_stg_detail, acc_card_stg_detail

    def gap_find_best_option(self, gap_pred_info,acc_budget):
        x = {}
        solver = pywraplp.Solver.CreateSolver('CBC')
        solver.max_time_in_seconds = 240.0
        for i in range(len(gap_pred_info)):
            x[i] = solver.NumVar(0.0, 1, 'x[%i]' % (i))
        pangu_remain_budget = self.params["gap_budget"] - acc_budget
        print("********留给盘古填坑的预算：**********",pangu_remain_budget)

        if pangu_remain_budget < 1000:
            raise Exception("pangu budget exceed lower bound !!!! ")

        solver.Add(solver.Sum([x[i] * gap_pred_info['subsidy'].iloc[i]
                               for i in range(0, len(x))]) <= self.params["gap_budget"] - acc_budget)
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
        solver.Minimize(solver.Sum([x[i] * gap_pred_info['abn_rate'].iloc[i] for i in range(0, len(x))]))
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
        print("B端填坑阶段 总花费 ：",gap_choose_scheme['subsidy'].sum())
        print("B端填坑总预算：",pangu_remain_budget )
        return gap_choose_scheme

    def get_round_b_rate(b_rate):
        b_rate1,b_rate2,b_rate3,b_rate4 = round(b_rate-0.002,3),round(b_rate-0.004,3),round(b_rate+0.002,3),round(b_rate+0.004,3)
        return b_rate,b_rate1,b_rate2,b_rate3,b_rate4
        
    
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
        
        delta_cr_h = pred_cr - x.cr  #给完补贴提升的CR 
        cr_gap_h = x.cr_bar - pred_cr # 给完补贴后距离CR_bar 
        pred_cr_h = x.cr_bar if pred_cr > x.cr_bar else pred_cr   #FIXME 后续步长较大的情形
        abn_rate_h = call_order_cnt_h * (pred_cr_h - x.cr_bar)**2  #成交异常率 
        subsidy_h = gmv_h * x.b_rate
        return gmv_h ,call_order_cnt_h,delta_cr_h, abn_rate_h,subsidy_h,cr_gap_h

    def get_gap_dignosis_info(self, max_try_num=7):
        '''
        获取供需诊断数据，兜底选择分区并判断是否为空
        '''
        for try_num_gap in range(max_try_num):
            self.params['gap_fillup_try_num'] = try_num_gap
            print("SQL of gap_dignosis: ",gap_dignosis_sql.format(**self.params))
            gap_dignosis_pre = self.hc.sql(gap_dignosis_sql.format(**self.params)).toPandas()

            # 首先判断分区是否有数据，没有进行分区兜底
            if gap_dignosis_pre.shape[0] != MIN_NUM_OF_DATA :
                return gap_dignosis_pre
            if(self.params['gap_fillup_try_num'] == max_try_num-1):
                raise Exception('get gap dignosis info fail !')

    def get_b_fillup_optimal_data(self, max_try_num=7):
        """
        :param max_try_num:
        :return: 获取B端填坑的基本信息表，聚合到城市粒度，并包含每个城市对应区县的不同时段加速卡和盘古活动
        """

        # pangu and acc card subsidy rate dict
        pangu_rate_dict = self.params["pangu_rate_dict"]
        accelerate_card_rate_dict = self.params["accelerate_card_rate_dict"]
        print("pangu_rate_dict & accelerate_card_rate_dict ：",pangu_rate_dict ,accelerate_card_rate_dict)
        json_pangu_rate_dict = json.dumps(pangu_rate_dict)
        json_accelerate_card_rate_dict = json.dumps(accelerate_card_rate_dict)

        #生成出屏蔽外的供需诊断数据
        bk_day_tool_list = self.generate_gap_bk_list()
        df_sample = self.hc.sql("select pred_date as stat_date,city_id from df_sample").toPandas()
        gap_dignosis_raw = self.get_gap_dignosis_info()
        gap_dignosis = gap_dignosis_raw.merge(df_sample,on=['stat_date','city_id'],how = 'inner')

        gap_dignosis['idx_temp'] = ['_'.join(i) for i in gap_dignosis[['city_id','stat_date','tool']].values.astype(str)] # 添加唯一idx ，作为该区县时段唯一标识
        gap_dignosis = gap_dignosis[~gap_dignosis['idx_temp'].isin(bk_day_tool_list)]

        if gap_dignosis.shape[0] == MIN_NUM_OF_DATA:
            gap_b_table = pd.DataFrame(columns=['city_id','pred_date','ori_b_budget','ori_acc_budget','pangu_stg_detail','acc_card_stg_detail'])
        else:      
            gap_dignosis = gap_dignosis[['city_id', 'stat_date', 'fence_id', 'tool', 'start_time', 'start_hour',
                                            'end_time', 'end_hour', 'cr', 'cr_bar', 'gmv_ratio',
                                            'finish_count_ratio', 'call_count_ratio']]
            print("供需诊断分布",gap_dignosis.groupby('stat_date').count())
            
            for try_num in range(max_try_num):
                """
                一，原始数据处理
                    1、盘古和加速卡弹性计算 ｜ 筛选对应时段，对应补贴率的数据
                    2、merge 供需诊断数据
                    3、加速卡对应补贴下的delta cr join  盘古供需数据，更新cr值
                    4、加速卡｜盘古，主要指标计算 
                二、规划求解
                三、聚合到城市维度，并将盘古和加速卡活动信息作为dict 拼接到新的字段中
                """
                self.params['b_fillup_try_num'] = try_num

                print("SQL of pred_and_pangu_elastic_sql:",pred_and_pangu_elastic_sql.format(**self.params))
                pangu_df = self.hc.sql(pred_and_pangu_elastic_sql.format(**self.params))
                pangu_elastic_df = pangu_df.withColumn('label',self.label_search_space_udf(json_pangu_rate_dict) \
                                                (pangu_df.city_id, pangu_df.b_rate, pangu_df.tool).cast('int')).filter("label == 1").drop("label")

                print("SQL of pred_and_acc_elastic_sql:",pred_and_acc_elastic_sql.format(**self.params))
                acc_df = self.hc.sql(pred_and_acc_elastic_sql.format(**self.params))
                acc_elastic_df = acc_df.withColumn('label',self.label_search_space_udf(json_accelerate_card_rate_dict) \
                                                ( acc_df.city_id, acc_df.b_rate, acc_df.tool).cast('int')).filter("label == 1").drop("label")

                pangu_elastic = pangu_elastic_df.toPandas() 
                pangu_elastic['delta_tsh'] = pangu_elastic['b_rate'] * 1.2
                print("盘古弹性天分布：",pangu_elastic.groupby('stat_date').count())

                # 根据运营规则进行修正 ，对ROI进行调整
                # pangu_elastic['delta_tsh'] = pangu_elastic[['delta_tsh','b_rate']].apply(lambda x : min(max(x.delta_tsh/x.b_rate,1),3) * x.b_rate,axis =1 ) 
                # print("**********修正后的delta_tsh 上下界*************",pangu_elastic['delta_tsh'].max(),pangu_elastic['delta_tsh'].min())
                
                acc_elastic = acc_elastic_df.toPandas()
                acc_elastic['b_rate'] = 0.04 # 临时调整，加速卡放量需求
                acc_elastic['delta_tsh'] = acc_elastic['b_rate'] * 1.2
                print("加速卡弹性天分布：",acc_elastic.groupby('stat_date').count())


                print("generate data and transform to pandas!!!")
                print("***********活动分布***********",gap_dignosis.groupby('tool').count())

                if(pangu_elastic.shape[0] == MIN_NUM_OF_DATA):
                    continue

                # 1、加速卡填坑
                acc_elastic_merge_df = pd.merge(gap_dignosis[gap_dignosis['tool'] =='加速卡'],acc_elastic[acc_elastic['b_rate'] != 0], on = ['city_id','stat_date','tool'],how ='inner') 
                if acc_elastic_merge_df.shape[0] != MIN_NUM_OF_DATA:
                    # acc_elastic_merge_df = pd.merge(gap_dignosis[gap_dignosis['tool'] =='加速卡'],acc_elastic[acc_elastic['b_rate'] != 0], on = ['city_id','stat_date','tool'],how ='inner') 
                    acc_elastic_merge_df = acc_elastic_merge_df[ acc_elastic_merge_df['stat_hour'] >= acc_elastic_merge_df['start_hour']][acc_elastic_merge_df['stat_hour'] <= acc_elastic_merge_df['end_hour'] ]
                    # acc_elastic_merge_df.to_csv('acc_elastic_merge_df.csv')  ## test 
                    acc_elastic_merge_df[['gmv_h' ,'call_order_cnt_h','delta_cr_h', 'abn_rate_h','subsidy_h','cr_gap_h']] = acc_elastic_merge_df.apply(self.get_hourly_indicator,axis=1,result_type = 'expand')
                    #  中间产物，通过该表获取加速卡活动后分小时的cr变化
                    acc_hourly_delta_cr = acc_elastic_merge_df[['city_id','stat_date','fence_id','tool','stat_hour','cr','cr_bar','delta_cr_h']] 
                    # 聚合到活动粒度,和盘古规划活动一起进行join，作为最后输出
                    acc_event_info =  acc_elastic_merge_df.groupby(['city_id','stat_date','fence_id','start_time','end_time','cr','cr_bar','b_rate'],as_index = False).agg(
                        {'gmv_h':'sum',
                        'abn_rate_h':'sum',
                        'subsidy_h':'sum',
                        'cr_gap_h':'mean'
                        }).rename(columns = {'gmv_h':'gmv','abn_rate_h':'abn_rate','subsidy_h':'subsidy','cr_gap_h':'cr_gap'})
                    acc_event_info['tool'] = '加速卡'
                    print("************加速卡填坑***************",acc_event_info.shape, acc_event_info.head())
                    print("************加速卡填坑花费***************",acc_event_info['subsidy'].sum())
                else:
                    acc_hourly_delta_cr = pd.DataFrame(columns=['city_id','stat_date','fence_id','tool','stat_hour','cr','cr_bar','delta_cr_h'])
                    acc_event_info = pd.DataFrame(columns=['city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr',
                                    'cr_bar', 'b_rate', 'gmv', 'abn_rate', 'subsidy', 'tool'])
                    print("加速卡活动为0")

                # 2、盘古填坑
                # 首先加工盘古活动的分小时弹性数据
                pangu_elastic_merge_df = pd.merge(gap_dignosis[gap_dignosis['tool'] =='智能盘古'],pangu_elastic, on = ['city_id','stat_date','tool'],how ='inner')
                pangu_elastic_merge_df  = pangu_elastic_merge_df[ pangu_elastic_merge_df['stat_hour'] >= pangu_elastic_merge_df['start_hour']][pangu_elastic_merge_df['stat_hour'] <= pangu_elastic_merge_df['end_hour'] ]
                # TODO 添加约束：如果活动时长超过8小时，对应的补贴率不要超过8pp 
                pangu_elastic_merge_df = pangu_elastic_merge_df.drop( pangu_elastic_merge_df[pangu_elastic_merge_df['end_hour'] - pangu_elastic_merge_df['start_hour'] >=8 ][pangu_elastic_merge_df['b_rate'] >=0.08].index )

                pangu_merge_acc_cr  = pd.merge(pangu_elastic_merge_df,acc_hourly_delta_cr , on = ['city_id','stat_date','stat_hour'], how = 'left',suffixes=('', '_acc'))
                pangu_merge_acc_cr['delta_cr_h'] = pangu_merge_acc_cr['delta_cr_h'].fillna(0)
                pangu_merge_acc_cr['cr_ori'] = pangu_merge_acc_cr['cr']  
                pangu_merge_acc_cr['cr'] = pangu_merge_acc_cr['cr_ori'] + pangu_merge_acc_cr['delta_cr_h']   # cr update  
                
                # 运营sence，添加规则，坑深大于 8pp ，补贴大于 5 ，坑小于 4pp 补贴小于10pp
                # pangu_merge_acc_cr = pangu_merge_acc_cr.drop( pangu_merge_acc_cr[pangu_merge_acc_cr['cr_bar'] - pangu_merge_acc_cr['cr'] < 0.04][pangu_merge_acc_cr['b_rate']>0.1].index) # 去掉 坑深小于4pp的，且给了高补贴的
                # pangu_merge_acc_cr = pangu_merge_acc_cr.drop( pangu_merge_acc_cr[pangu_merge_acc_cr['cr_bar'] - pangu_merge_acc_cr['cr'] > 0.08][pangu_merge_acc_cr['b_rate']<0.05].index ) # 去掉坑深大于8pp的， 且给了低补贴的

                df = pangu_merge_acc_cr.drop(labels=['fence_id_acc','fence_id_acc','tool_acc','cr_acc','cr_bar_acc','delta_cr_h'], axis=1)
                # 指标加工
                df[['gmv_h' ,'call_order_cnt_h','delta_cr_h', 'abn_rate_h','subsidy_h','cr_gap_h']] = df.apply(self.get_hourly_indicator,axis=1,result_type = 'expand')
                # 聚合到活动粒度,然后进行规划
                event_info =  df.groupby(['city_id','stat_date','fence_id','start_time','end_time','cr','cr_bar','b_rate'],as_index = False).agg(
                    {'gmv_h':'sum',
                    'abn_rate_h':'sum',
                    'subsidy_h':'sum',
                    'cr_gap_h':'mean'
                    }).rename(columns = {'gmv_h':'gmv','abn_rate_h':'abn_rate','subsidy_h':'subsidy','cr_gap_h':'cr_gap'})
                
                # 去除活动补贴后大于cr bar 
                # event_info = event_info[event_info['cr_gap'] >=0 ]
                event_info['tool'] = '智能盘古'


                print("盘古填坑数据： data info",event_info.shape,event_info.head())
                print("盘古填坑天分布",event_info.groupby("stat_date").count())

                raw_event_num = event_info.shape[0]
                if(raw_event_num > 1):
                    break


            # 填坑规划求解
            gap_pred_info = event_info.reset_index(drop = True)
            gap_pred_info['idx'] = list(range(gap_pred_info.shape[0]))
            gap_pred_info['group_idx'] = ['_'.join(i) for i in gap_pred_info[['start_time','city_id','fence_id','stat_date']].values.astype(str)] # 添加唯一idx ，作为该区县时段唯一标识
            print("盘古填坑规划准备数据 info：",gap_pred_info.shape,gap_pred_info.head())
            # gap_pred_info.to_csv('check_data_gap_before_op.csv') # gallin test
            gap_choose_scheme = self.gap_find_best_option(gap_pred_info,acc_event_info['subsidy'].sum())        
            # 3、加速卡和盘古结果融合
            # gap_choose_scheme.to_csv('check_data_pangu_after_op.csv') # gallin test

            gap_choose_scheme = gap_choose_scheme[gap_choose_scheme['subsidy'] != 0] # 取实际花钱的活动
            if acc_elastic.shape[0] != MIN_NUM_OF_DATA:
                gap_choose_scheme = pd.concat([gap_choose_scheme[acc_event_info.columns],acc_event_info])
            else:
                gap_choose_scheme = gap_choose_scheme[['city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr',
                                    'cr_bar', 'b_rate', 'gmv', 'abn_rate', 'subsidy', 'tool']]
            print("加速卡和盘古结果活动总数：",gap_choose_scheme.shape)
            
            # 20220526 最终的补贴活动信息 dashboard_v1
            daily_gmv_info = self.hc.sql(daily_gmv_sql.format(**self.params)).toPandas() 
            event_allocation_info = pd.merge(gap_choose_scheme,daily_gmv_info, on = ['city_id','stat_date'],how = 'left')
            event_allocation_info = event_allocation_info[['city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr',
                                    'cr_bar', 'b_rate','subsidy','tool', 'gmv', 'gmv_daily']]
            event_allocation_info['b_rate_daily'] = event_allocation_info['subsidy']/ event_allocation_info['gmv_daily']
            event_allocation_info.rename(columns = {'b_rate':'时段补贴率','tool':'活动类型','subsidy':'补贴额','b_rate_daily':'全天补贴率','gmv_daily':'全天gmv'})
            print(event_allocation_info.head())
            gift_link_event = upload_to_gift(event_allocation_info,"预算分配报告","b_allocation_event_info")
            send_message("B端分配活动补贴信息",self._stg_param['is_online'], gift_link_event )
            
            gap_choose_scheme[["pangu_stg_detail","acc_card_stg_detail"]] = gap_choose_scheme.apply(self.agg_event_2_city_options,axis=1,result_type = 'expand')
            gap_b_table = self.return_b_gap_info(gap_choose_scheme)

            gap_b_table = gap_b_table[['city_id','pred_date','ori_b_budget','ori_acc_budget','pangu_stg_detail','acc_card_stg_detail']]
        
        # 20220526 最终的补贴城市信息 dashboard_v1
        city_allocation_info = pd.merge(gap_b_table,daily_gmv_info, on = ['city_id','pred_date'],how = 'left')
        city_allocation_info = city_allocation_info[['city_id','pred_date','ori_b_budget','ori_acc_budget','gmv_daily']]
        city_allocation_info['b_rate'] = city_allocation_info['ori_b_budget']/ city_allocation_info['gmv_daily']
        city_allocation_info['acc_rate'] = city_allocation_info['ori_acc_budget']/ city_allocation_info['gmv_daily']
        city_allocation_info.rename(columns = {'b_rate':'盘古全天补贴率','acc_rate':'加速卡全天补贴率','ori_b_budget':'盘古补贴额','ori_acc_budget':'加速卡补贴额','gmv_daily':'全天gmv'})
        print(city_allocation_info.head())
        gift_link_city = upload_to_gift(city_allocation_info,"预算分配报告","b_allocation_city_ifo")
        send_message("B端分配城市补贴信息",self._stg_param['is_online'], gift_link_city )
        


        print("填坑结果-gap_b_table:",gap_b_table.head())
        schema = StructType([StructField("city_id", IntegerType(), True),  StructField("pred_date", StringType(),True ), StructField("ori_b_budget", DoubleType(), True),StructField("ori_acc_budget", DoubleType(), True), \
                        StructField("pangu_stg_detail", StringType(), True),StructField("acc_card_stg_detail", StringType(), True)])
        gap_b_table_df = self.hc.createDataFrame(gap_b_table ,schema = schema)

        return gap_b_table_df 


    def get_b_fillup_month_data(self, max_try_num=7):
        # b端填坑表扩充为月级别数据，对最后一天复制多份。
        st_end_times = sorted(self.params['st_end_times'])
        start_time, end_time = st_end_times[0], st_end_times[-1]
        for try_num in range(max_try_num):
            self.params['b_fillup_try_num'] = try_num
            b_table = self.hc.sql(b_fillup_sql.format(**self.params)).cache()
            if(b_table.count() == MIN_NUM_OF_DATA):
                continue
            b_table_info = b_table.select([F.max(col('pred_date')).alias(
                'max'), F.min(col('pred_date')).alias('min')]).collect()[0]
            print('b_table max_date: {}, min_date:{}'.format(
                b_table_info.max, b_table_info.min))
            max_date_df = b_table.filter(
                'pred_date == "{}"'.format(b_table_info.max)) 
            b_table_dfs = [b_table]
            for i in range(1, num_of_expand_days, 1):
                df_elem = max_date_df.withColumn('pred_date', date_add(col('pred_date'), i))\
                    .withColumn('pred_date', col('pred_date').astype("string"))
                b_table_dfs.append(df_elem)
            month_level_b_table_df = reduce(DataFrame.unionAll, b_table_dfs)
            month_level_b_table_df = month_level_b_table_df.filter("pred_date in ({st_end_times_str}) ".format(**self.params))

            pred_start_num = month_level_b_table_df.filter(
                'pred_date == "{}"'.format(start_time)).count()
            pred_end_num = month_level_b_table_df.filter(
                'pred_date == "{}"'.format(end_time)).count()
            print(pred_start_num, pred_end_num)
            if(pred_end_num > MIN_NUM_OF_DATA):
                print('b elasticity backup num: {},{}'.format(
                    pred_start_num, pred_end_num))
                return month_level_b_table_df
        
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
            print(pred_start_num, pred_end_num)
            if(pred_start_num > 1 and pred_end_num > 1):
                pred_dapan_info.registerTempTable("pred_dapan_info_gmv")
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
            raise Exception('get pred info fail !')
        if(self.params['alpha_try_num'] == max_try_num-1):
            raise Exception('get alpha info fail !')

    def get_c_elasticity(self, max_backup_num=7):
        # c端弹性，backup方案，往前找。
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
                return month_level_c_delta_send_rate_df
        raise Exception('c tanging have problem!')

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
        raise Exception('b tanging have problem!')

    def get_history_diaodu_scope(self, max_try_num=7):
        for try_num in range(max_try_num):
            print('try_num:',try_num)
            self.params['try_num'] = try_num
            print(diaodu_sql.format(**self.params))
            pred_diaodu_info = self.hc.sql(diaodu_sql.format(**self.params))
            if(pred_diaodu_info.count()>0):
                return pred_diaodu_info 
        if(self.params['try_num'] == max_try_num-1):
            raise Exception('get diaodu info fail !')

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
            raise Exception('get diaodu lastweek info fail!!!')

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

    def get_c_fillup_tab(self):
        '''
        :return:  C端填坑弹性表 C端填坑表
        '''
        # 1.得到月级别的c端填坑表
        month_level_fillup_df = self.get_c_fillup_month_data()
        month_level_fillup_df.registerTempTable('month_level_fillup_df')
        c_rate = self.hc.sql(pred_and_c_fillup_sql.format(**self.params))
        print("***************c_rate.show() C端填坑补贴率***************")
        print(c_rate.show(5))

        # 2. 弹性表
        month_level_c_delta_send_rate_df = self.get_c_elasticity()
        print("***************C端弹性表数据******************")
        print(month_level_c_delta_send_rate_df.show(5))
        print('c elasticity count:', month_level_c_delta_send_rate_df.count())
        # 3. 填坑表和弹性表拼接
        # 填坑补贴率+bc平衡补贴率 属于（填坑补贴率，39），如果 填坑补贴率> 39, 填坑补贴率+bc平衡补贴率 = 填坑补贴率
        # treat_subsidy_hufan_rate_new = 填坑补贴率+bc平衡补贴率 
        # 屏蔽策略:  填坑补贴率+bc平衡补贴率 = 0.0

        # 保留的弹性补贴率： 新的补贴率（填坑补贴率+bc平衡的补贴率） 可选项：   0.0，  填坑的补贴率， （填坑的补贴率，39）
        #                 新的补贴率 >= 填坑补贴率
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
        print('c candidate number:', c_data.count())
        c_data = c_data.withColumn('realloc_budget_c', col(
            'pukuai_gmv')/1000 * (col('treat_subsidy_hufan_rate_new')-col('treat_subsidy_hufan_rate')))

        # 4 得到历史调度上下区间
        history_diaodu_scope = self.get_history_diaodu_scope()
        c_data = c_data.join(history_diaodu_scope,(c_data['city_id'] == history_diaodu_scope['city_id']) &
                                              (c_data['day_of_week'] == history_diaodu_scope['day_of_week'])
                                             ,'left'
                        ).drop(history_diaodu_scope['city_id']).drop(history_diaodu_scope['day_of_week'])
        print('**************C端填坑合并弹性表结果数据*****************')
        print(c_data.show(10))

        return c_data, c_rate

    def c_table_merge_b_table(self, c_data):

        # 得到b端月级别填坑表。
        # b_table = self.get_b_fillup_month_data()  # 加速卡调整 1125
        b_table = self.get_b_fillup_optimal_data() # 采用新的填坑规划数据 1125
        print('****************得到新的B端填坑规划结果 count and head20************: ', b_table.count())
        # b_table.write.mode('overwrite').saveAsTable('bigdata_driver_ecosys_test.temp_b_table_test')
        print(b_table.show(20))

        if b_table.count() == 0:
            self.params['b_spent'] = 0 
        else:
            self.params['b_spent'] =  b_table.agg({'ori_b_budget': 'sum'}).collect()[
                0]['sum(ori_b_budget)'] + b_table.agg({'ori_acc_budget': 'sum'}).collect()[
                0]['sum(ori_acc_budget)']
        # 和c表拼接
        print("**********c_data**************")
        print(c_data.show(10))
        # c_data.write.mode('overwrite').saveAsTable('bigdata_driver_ecosys_test.temp_c_data')
        c_b_data = c_data.join(b_table, (c_data['city_id'] == b_table['city_id']) &
                               (c_data['pred_date'] == b_table['pred_date']), 'left').drop(b_table['city_id']).drop(b_table['pred_date'])\
            .withColumn('ori_b_rate', (col('ori_b_budget')/col('total_gmv') * 1000).cast("int")).withColumn('ori_acc_rate', (col('ori_acc_budget')/col('total_gmv') * 1000).cast("int"))  # 生成对应原始补贴率
        c_b_data = c_b_data.na.fill(subset='ori_b_budget', value=0.0)
        c_b_data = c_b_data.na.fill(subset='ori_b_rate', value=0.0)
        c_b_data = c_b_data.na.fill(subset='ori_acc_budget', value=0.0)
        c_b_data = c_b_data.na.fill(subset='ori_acc_rate', value=0.0)
        print(" After merge B&C, and filter all null data , any null data bellow : ",c_b_data.filter('ori_b_budget is null ').show())
        print("************合并BC表以后，结果如下,接下来合并B表弹性************") 
        print(c_b_data.show(5))

        # 拿到弹性结果
        b_elasticity_df = self.get_b_elasticity() #TODO 如果B端填坑的预算为空，那么对应的b端弹性也一定是0（只在坑里面进行平衡预算分配）
        b_elasticity_df.write.mode('overwrite').saveAsTable('bigdata_driver_ecosys_test.temp_b_elasticity_df')
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
        c_b_data = c_b_data.withColumn('realloc_b_budget', col('new_b_rate') * 0.0)

        c_b_data.registerTempTable('ori_table')
        print('b and c merge!, count:', c_b_data.count())
        print(c_b_data.show(2))
        return b_table

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
        pred_info = pred_info.na.fill(subset='pangu_stg_detail', value=json.dumps([]))
        pred_info = pred_info.na.fill(subset='acc_card_stg_detail', value=json.dumps([]))

        def cal_succ_rate(infos,total_columns):
            gongxu_max,gongxu_min = infos.gongxu_max, infos.gongxu_min
            diaodu_b_max,diaodu_b_min = infos.diaodu_b_max, infos.diaodu_b_min

            diaodu_min_val, diaodu_max_val = infos.subsidy_money_lowerbound, infos.subsidy_money_upperbound
            gongxu, total_gmv = infos.gongxu, infos.total_gmv
            if diaodu_max_val - diaodu_min_val < 1: #equal, reset min, max
                if diaodu_max_val > 0:
                    diaodu_min_val = 0.0
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
            # 加入调度 0.0, 是为了应对屏蔽策略。
            for cur_td_subsidy in sorted(set([0.0] + list(np.linspace(diaodu_min_val, diaodu_max_val,10)))):
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

        # df_process.write.mode('overwrite').saveAsTable('bigdata_driver_ecosys_test.temp_df_process_bc_strategy')
        # df_process=self.hc.table("bigdata_driver_ecosys_test.temp_df_process_bc_strategy")
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
        df_process.write.mode('overwrite').saveAsTable('bigdata_driver_ecosys_test.temp_df_process_final_test')
        return df_process

        # df_process.toPandas.to_csv('gallin_test_1209_df_process.csv')

        # df_process = df_process.filter('gmv is null ')

        def get_dgmv(infos):
            key, city_day_rows = infos
            ori_gmv = 1.0
            for elem in city_day_rows:
                # todo: cur_diaodu_subsidy = 0
                if((elem.ori_c_rate == elem.new_c_rate) & (elem.ori_b_budget == elem.new_b_budget) & (elem.subsidy_money_lowerbound == elem.cur_diaodu_subsidy)):
                    ori_gmv = elem.gmv
            outputs = []
            for elem in city_day_rows:
                dgmv = elem.gmv/(ori_gmv + 1e-12) - 1
                output = []
                for col_n in total_columns:
                    output.append(elem[col_n])
                output.append(dgmv)
                outputs.append(output)
            return outputs

        total_columns = df_process.columns

        print("total_columns:",total_columns)
        df_process = df_process.rdd.groupBy(lambda x: (x.city_id, x.pred_date)).mapValues(list) \
            .flatMap(get_dgmv).toDF(total_columns+['dgmv'])
        return df_process

    def preprocessed_data_save(self):
        ###  数据生成的模块
        # 1. 得到输入参数
        city_list, cur_day, start_date, end_date, trace_id, total_budget, holiday_set, remove_date, gap_rate, accelerate_card_rate_dict, pangu_rate_dict = self.get_global_infos()
        self.params = {
            'cur_day': cur_day,
            'start_time': start_date,
            'end_time': end_date,
            'trace_id': trace_id,
            'city_list': ",".join(map(str, city_list)),
            'holiday_set': holiday_set,
            'gap_budget': gap_rate * total_budget,
            'accelerate_card_rate_dict': accelerate_card_rate_dict,
            'pangu_rate_dict':pangu_rate_dict
        }
        pred_dates = list(pd.date_range(start_date, end_date).astype('str'))
        if len(pred_dates) < 1:
            raise Exception('num of days is zero !')
        self.params['st_end_times'] = pred_dates
        self.params['st_end_times_str'] = str(pred_dates)[1:-1]
        print(self.params['st_end_times'])
        print(self.params['st_end_times_str'])
        print('B端填坑总预算：',self.params['gap_budget'] )

        date_city_index = pd.MultiIndex.from_product([pred_dates, city_list], names=["pred_date", "city_id"])
        df_sample = self.hc.createDataFrame(pd.DataFrame(index=date_city_index).reset_index())
        df_sample.registerTempTable('df_sample')
        
        stat_hour_list = [int(i) for i in np.linspace(6,23,18)]
        b_rate_list = [round(i,3) for i in np.linspace(0.0,0.15,151)]
        multi_index = pd.MultiIndex.from_product([city_list,pred_dates,stat_hour_list,b_rate_list], names=['city_id', 'start_dt','start_time','b_rate'])
        base_ela_df = pd.DataFrame(index=multi_index).reset_index()
        base_ela_df['estimate'] = base_ela_df['b_rate'] * 1.2  # 弹性
        base_ela_py_df = self.hc.createDataFrame(base_ela_df)
        base_ela_py_df.registerTempTable('fake_b_ela_table')

        # 2. 确认网约车预测表和完单率模型
        self.get_pred_and_alpha_num()
        self.params['pred_sql'] = pred_sql.format(**self.params)
        self.params['alpha_sql'] = alpha_sql.format(**self.params)

        print("打印参数：",self.params)
        # 3. 拿到c端填坑表和c端弹性
        c_data, c_ori = self.get_c_fillup_tab()
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
        #c_data.write.mode('overwrite').saveAsTable('bigdata_driver_ecosys_test.temp_c_data')
        # 4. 拿到b端填坑表和b端弹性。 todo 修改b端填坑
        b_table = self.c_table_merge_b_table(c_data)
        
        # 5. 计算完单率和dgmv
        choose_rows = self.process_c_b_table()

        # 6.加一些过滤条件。 
        # 1、原始B补率不为0，但是c的补贴率为小于21 ，或者平衡的c小于5。 2、原始B补率为0的  
        choose_rows = choose_rows.filter("((ori_b_budget!= 0) and ( (new_c_rate <= 21) or (new_c_rate-ori_c_rate < 5 ) ) )\
                                           or (ori_b_budget == 0) ")
        # 7.保存表信息。
        print("******用于第二部进行roi规划的数据******",choose_rows.count(),choose_rows.show(10))
        print("*****填坑总预算 和 目前已花（加速卡+盘古）****", self.params["gap_budget"],self.params["b_spent"])
        print("******table will save in path******** : ",self._config['bc_table_path'].format(cur_day))
        choose_rows.repartition(10).write.mode('overwrite')\
                                                              .option('header', 'true')\
                                                              .csv(self._config['bc_table_path']
                                                                   .format(cur_day))
        return
    def preprocessed_data_remove(self):
        
        some_paths = [self._config['bc_table_path'].format(self.params['cur_day'])]
        for some_path in some_paths:
            print('delete hdfs path: ',some_path)
            subprocess.call(["hadoop", "fs", "-rm", "-r", some_path])
    
    def event_upper_bound_lable(self, x):
        """
        检查，放行活动补贴率 在12pp的活动 
        如果填坑预算大于12，采用 12，
        """
        UP_FACTOR = 0.12
        new_b_upper_bound = 0
        if len(json.loads(x.pangu_stg_detail)) == 0:
            return x.new_b_budget
        else:
            pg_stg_detail = json.loads(x.pangu_stg_detail)
            for stg_event in pg_stg_detail:
                new_b_upper_bound += json.loads(stg_event['param'])['gmv'] * UP_FACTOR
        new_b_upper_bound = max(new_b_upper_bound,x.ori_b_budget)
        return new_b_upper_bound

    def rescale_b_gap_money(self,x):
        """
        对活动中的预算进行处理， 将平衡的钱按照每个活动的gmv权重分发
        """

        # 处理盘古
        if len(json.loads(x.pangu_stg_detail))!= 0:
            stg_detail = json.loads(x.pangu_stg_detail)
            sum_amount = 0
            sum_gmv = 0

            for stg_event in stg_detail:
                sum_amount += stg_event['amount']
                sum_gmv += json.loads(stg_event['param'])['gmv']
            amount_gap = x.new_b_budget - sum_amount

            for stg_event in stg_detail:
                gmv_i = json.loads(stg_event['param'])['gmv']
                amount_i =  stg_event['amount'] 
                stg_event['amount'] = ((gmv_i/sum_gmv) * amount_gap + amount_i) * MONEY_UNIT
                stg_event_param = json.loads(stg_event['param'])
                stg_event_param['gmv'] = stg_event_param['gmv']* MONEY_UNIT
                stg_event['param'] = json.dumps(stg_event_param)

            stg_detail = json.dumps(stg_detail)
        else:
            stg_detail = json.dumps([])
        # 处理加速卡 
        if len(json.loads(x.acc_card_stg_detail))!= 0:
            acc_stg_detail = json.loads(x.acc_card_stg_detail)
            for acc_stg_event in acc_stg_detail:
                acc_stg_event['amount'] = acc_stg_event['amount'] * MONEY_UNIT
                acc_stg_event_param = json.loads(acc_stg_event['param'])
                acc_stg_event_param['gmv'] = acc_stg_event_param['gmv']* MONEY_UNIT
                acc_stg_event['param'] = json.dumps(acc_stg_event_param)
            acc_stg_detail = json.dumps(acc_stg_detail)
        else:
            acc_stg_detail = json.dumps([])

        return stg_detail,acc_stg_detail

    def get_bc_weight(self,max_backup_num=7):
        # 根据过去一段时间的供需情况，决定bc分配比列的占比。
        gongxu_quantiles = [0.3261695057153702,0.3394077569246292,0.35031357407569885, 0.363951712846756,0.3703732192516327,0.386654794216156]
        c_rate = [0.25, 0.33, 0.4, 0.5, 0.6, 0.66, 0.75]
        c_rate = [0.24, 0.25, 0.26, 0.27, 0.28, 0.29, 0.3]
        for try_num in range(max_backup_num):
            self.params['try_num'] = try_num
            gongxu_val = self.hc.sql(gongxu_sql.format(**self.params)).collect()[0].gongxu_val
            if gongxu_val is not None:
                val = 0
                for i in gongxu_quantiles:
                    if gongxu_val > i:
                        val += 1
                c_weight, b_weight = c_rate[val], 1-c_rate[val]
                self.params['c_weight'] = c_weight
                self.params['b_weight'] = b_weight
                print('recent gongxu: {}, b_weight: {}, c_weight:{}'.format(
                                        gongxu_val,b_weight,c_weight))
                # temporary tasks, change the B：C ratio to 1:2
                #add diaodu weight here
                #self.params['d_weight'] = 0.0586
                self.params['d_weight'] = 0.08
                self.params['c_weight'] = 0.565 * (1 - self.params['d_weight']) # 0.565 = 1.3/2.3
                self.params['b_weight'] = 1 - self.params['c_weight'] - self.params['d_weight']
                print('recent gongxu: {}, b_weight: {}, c_weight:{}'.format(
                        gongxu_val,self.params['b_weight'],self.params['c_weight']))
                return
        raise Exception('get_bc_weight fail !')

    def read_spark_csv_file(self):
        # 读取spark文件，过滤机制
        # total_black_str = self.generate_backlist_str()
        # 20211231 重写屏蔽规则
        total_black_dict = self.generate_backlist_dict() 
        print("参数打印：",self.params)
        print(" **************** raw_data path *************** ",self._config['bc_table_path'].format(self.params['cur_day']))
        df_process_temp = self.hc.read.csv(self._config['bc_table_path'].format(self.params['cur_day']), header=True)
        print("df_process_temp count :",df_process_temp.count())

        df_process = df_process_temp.withColumn('bk_label',self.label_bk_city_day_udf(total_black_dict)(df_process_temp.pred_date,df_process_temp.city_id, \
                                    df_process_temp.new_b_rate,df_process_temp.ori_acc_rate,df_process_temp.new_c_rate,df_process_temp.cur_diaodu_subsidy
                                    ).cast('int')).filter("bk_label != 1").drop("bk_label").toPandas().reset_index(drop=True)


        print("df_process shape: ",df_process.shape, df_process.head(3))
        # 数据类型转化
        df_process.city_id = df_process.city_id.astype(int)
        if(len(df_process) == 0):
            raise Exception('bc_table_path have exceptions!')

        process_double_columns = ['total_gmv','ori_c_rate', 'new_c_rate', 'pred_dsend_ratio', 'ori_c_budget', 'budget_c_gap', 'ori_b_budget',
                              'ori_b_rate','ori_acc_budget','ori_acc_rate','new_b_budget', 'budget_b_gap', 'new_b_rate', 'base_online_time', 'asp', 'call_order_cnt',
                              'new_call_order_cnt', 'new_online_time', 'k1', 'k2', 'k3','gongxu_max','gongxu_min', 'diaodu_b_max','diaodu_b_min',
                              'gongxu','gongxu_normalization','cur_diaodu_normalization_rate', 'cur_diaodu_subsidy','subsidy_money_lowerbound',
                              'subsidy_money_upperbound','lastweek_avg_diaodu_rate', 'lastweek_diaodu_gmv', 'succ_rate','gmv', 'dgmv']
        # print(len(decimalCols))
        for colname in process_double_columns:
            df_process[colname] = df_process[colname].astype(float)
        df_process['budget_b_gap'] = df_process.apply(lambda x: x.budget_b_gap if x.budget_b_gap >= 0 else 0.0,axis=1) #TODO check if wrong data from upside
        df_process['budget_c_gap'] = df_process.apply(lambda x: x.budget_c_gap if x.budget_c_gap >= 0 else 0.0,axis=1)
        # 剔除cur_diaodu_normalization_rate ==0 但是 cur_diaodu_subsidy 不为0 的
        df_process = df_process.drop( df_process[df_process['cur_diaodu_normalization_rate'] ==0 ][df_process['cur_diaodu_subsidy'] !=0].index )
        print('loading data from spark path , df_process:',df_process.head(5))
        return df_process

    def find_optimal_combination(self):
        ### 线性规划求解
        # 1. 拿到输入参数
        city_list, cur_day, start_date, end_date, trace_id, total_budget, holiday_set, remove_date,\
                    gap_rate, accelerate_card_rate_dict, pangu_rate_dict = self.get_global_infos()
        self.params = {
            'cur_day': cur_day,
            'start_time': start_date,
            'end_time': end_date,
            'trace_id': trace_id,
            'city_list': ",".join(map(str, city_list)),
            'holiday_set': holiday_set
        }
        print("***************total_budget****************",total_budget)
        pred_dates = list(pd.date_range(start_date, end_date).astype('str'))
        if len(pred_dates) < 1:
            raise Exception('num of days is zero !')
        self.params['st_end_times'] = pred_dates
        self.params['st_end_times_str'] = str(pred_dates)[1:-1]
        print(self.params['st_end_times'])
        print(self.params['st_end_times_str'])
        date_city_index = pd.MultiIndex.from_product([pred_dates, city_list], names=["pred_date", "city_id"])
        df_sample = pd.DataFrame(index=date_city_index).reset_index()
        
        # 2. 读取参数指定的数据
        choose_rows = self.read_spark_csv_file()
        print("choose_rows",choose_rows.head())

        # 3. 拿到b和c的填坑金额
        # 屏蔽的数据不统计填坑的钱
        choose_rows['ori_c_budget'] = choose_rows.apply(lambda x: x.ori_c_budget if x.new_c_rate != 0.0 else 0.0, axis=1)
        # choose_rows['ori_b_budget'] = choose_rows.apply(lambda x: x.ori_b_budget if x.new_b_rate != 0.0 else 0.0, axis=1)
        choose_rows1 = choose_rows[['city_id','pred_date','ori_c_budget','ori_b_budget','ori_acc_budget']].drop_duplicates()
        print('size of choose row :', choose_rows1.shape)

        self.params['c_spent'] = choose_rows1.ori_c_budget.astype(float).sum()
        self.params['b_spent'] = choose_rows1.ori_b_budget.astype(float).sum()  
        self.params['acc_spent'] = choose_rows1.ori_acc_budget.astype(float).sum()
        print('统计填坑阶段已经花的钱：C端: {}, B端盘古: {}, 加速卡: {}'.format(self.params['c_spent'], self.params['b_spent'], self.params['acc_spent']))
        print("*************打印参数：***************",self.params)

        # 选择符合的调度
        choose_rows['subsidy_money_upperbound1'] = choose_rows.apply(lambda x: max(x.subsidy_money_upperbound, x.total_gmv * x.diaodu_b_max),axis=1)
        choose_rows = choose_rows.query('(cur_diaodu_subsidy < subsidy_money_upperbound1) or  \
                                        (cur_diaodu_subsidy == subsidy_money_lowerbound) ')
        # choose_rows.to_csv("gallin_test_choose_row.csv")
        
        # TODO 选择符合的盘古， 如果天级别的补贴对应的活动补贴率高于15pp，去除
        choose_rows['new_b_up_sub'] = choose_rows.apply(self.event_upper_bound_lable,axis=1,result_type = 'expand')
        choose_rows = choose_rows[choose_rows['new_b_budget'] <= choose_rows['new_b_up_sub']]

        # 4. 数据的预处理
        opti_data = choose_rows.reset_index(drop=True)
        opti_data['idx'] = list(range(opti_data.shape[0]))
        # opti_data['dt_gap'] = opti_data.apply(lambda x: x.cur_diaodu_subsidy -x.subsidy_money_lowerbound,axis=1)
        opti_data['dt_gap'] = opti_data['cur_diaodu_subsidy'] - opti_data['subsidy_money_lowerbound']

        #如果出现调度屏蔽，会有cur_diaodu_sub = 0，但是diaodu lower bould 不为0的情况
        opti_data['subsidy_money_lowerbound'] = opti_data.apply(lambda x: 0.0 if x.dt_gap < 0 else x.subsidy_money_lowerbound, axis=1)
        opti_data['dt_gap'] = opti_data.apply(lambda x: 0.0 if x.dt_gap < 0 else x.dt_gap, axis=1)

        opti_data['total_cost'] = opti_data['budget_c_gap'] + opti_data['budget_b_gap']  # balance 阶段总包
        opti_data['dgmv'] = opti_data.apply(
            lambda x: x.dgmv if x.dgmv > 0 else 0.0, axis=1)
        # opti_data['dgmv'] = opti_data.apply(lambda x: x.dgmv if x.s_rate > 0.001 else 0.0, axis = 1)
        self.params['ori_total_budget'] = total_budget
        dt_df = opti_data[['city_id','pred_date','subsidy_money_lowerbound']].drop_duplicates()
        self.params['dt_min_spent'] = dt_df.subsidy_money_lowerbound.sum()
        remain_budget = total_budget - self.params['b_spent'] - self.params['acc_spent'] - self.params['c_spent'] - self.params['dt_min_spent']
        print('总包金额：{}, 剩余金额: {}, B端已花: {}, 加速卡已花: {}, 调度已花：{}, C端已花：{}'.format(
            total_budget, remain_budget, self.params['b_spent'],self.params['acc_spent'],self.params['dt_min_spent'], self.params['c_spent']))
        # 5. 计算bc权重。
        self.get_bc_weight()  # todo 需要调整，不再关心
        # choose_scheme = use_day_contraint_opt(opti_data, remain_budget,self.params)
        # 6. 线性规划求解
        choose_scheme = linear_contraint(opti_data, remain_budget, self.params)
        # total_content = pd.merge(ori_c_table, choose_scheme[['city_id', 'pred_date', 'budget_c_gap', 'budget_b_gap','succ_rate', 'gmv', 'dgmv']],
        #                          on=['city_id', 'pred_date'], how='left')
        # total_content = pd.merge(total_content, ori_b_table, left_on=['city_id', 'pred_date'],
        #                          right_on=['city_id', 'pred_date'], how='left')
        total_content = choose_scheme.copy()
        # choose_scheme.to_csv('roi_opt_choose_scheme.csv')

        total_content.budget_b_gap = total_content.budget_b_gap.fillna(0.0)
        total_content.budget_c_gap = total_content.budget_c_gap.fillna(0.0)
        # total_content.time_segment_infos = total_content.time_segment_infos.fillna([])
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
        # total_content.to_csv('total_content_gallin_test01.csv')
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
        daily_optimal_result = operation_rule(total_budget, total_content, self.params)
        #change the diaodu 0.0 budget to minimun budget
        gmvdata = opti_data.query("cur_diaodu_subsidy > 0")[['city_id', 'pred_date','total_gmv']].drop_duplicates()
        totalgmv = np.sum(gmvdata['total_gmv'])
        diaodutotal = np.sum(daily_optimal_result['cur_diaodu_subsidy'])
        gmvdata['diaodu_def'] = gmvdata.apply(lambda x : x.total_gmv/totalgmv * 0.2 * diaodutotal , axis = 1)
        if 'total_gmv' in gmvdata:
            del gmvdata['total_gmv']
        my_daily_optimal_result = pd.merge(daily_optimal_result, gmvdata, how = 'left', on = ['city_id', 'pred_date'])
        my_daily_optimal_result['cur_diaodu_subsidy'] = my_daily_optimal_result.apply(lambda x : x.cur_diaodu_subsidy * 0.8 + x.diaodu_def, axis= 1)
        daily_optimal_result = my_daily_optimal_result.copy()

        # 聚合
 
        week_optimal_result = daily_optimal_result.groupby('city_id').agg({'pred_date': ['min'], 'ori_c_budget': ['sum'], 'new_c_budget': ['sum'], 
                                                                           'ori_b_budget': ['sum'], 'new_b_budget': ['sum'],'cur_diaodu_subsidy':['sum']
                                                                    ,'succ_rate':['mean'], 'total_gmv': ['sum'], 'dgmv': ['mean']}).reset_index()
        week_optimal_result.columns = ['city_id', 'pred_date', 'ori_c_budget','new_c_budget', 'ori_b_budget',
                                        'new_b_budget','cur_diaodu_subsidy','succ_rate', 'total_gmv', 'dgmv']
        # save_df = self.hc.createDataFrame(week_optimal_result[['city_id', 'pred_date', 'ori_c_budget', 'new_c_budget',
        #                                   'ori_b_budget', 'new_b_budget','succ_rate', 'total_gmv', 'dgmv']])
        week_optimal_result['new_c_budget_rate'] = week_optimal_result['new_c_budget'] / week_optimal_result['total_gmv']
        week_optimal_result['new_b_budget_rate'] = week_optimal_result['new_b_budget'] / week_optimal_result['total_gmv']
        week_optimal_result['new_diaodu_budget_rate'] = week_optimal_result['cur_diaodu_subsidy'] / week_optimal_result['total_gmv']

        week_optimal_result['roi'] = week_optimal_result.apply(lambda x: x.dgmv * x.total_gmv /(x.new_c_budget + x.new_b_budget + 1e-14),axis=1)
        print('周级别数据聚合：')
        print("week_optimal_result: ",week_optimal_result.head())
        # save_df_detail = self.hc.createDataFrame(daily_optimal_result)
        # save_df_detail.repartition(1).write.mode('overwrite').option('header', 'true')\
        #     .csv(self._config['daily_result_path'].format(self._stg_param['is_online'], self.params['cur_day'], self.params['trace_id']))
        # print('save path: ', self._config['total_result_path'].format(
        #     self._stg_param['is_online'], self.params['cur_day'], self.params['trace_id']))
        print("####11#####temp_df.count",daily_optimal_result.shape[0])
        daily_optimal_result.drop('lastweek_diaodu_gmv', axis=1, inplace=True)
        this_week_idx, last_week_idx = self.diaodu_index_df()
        lw_diaodu_info = self.get_diaodu_lastweek_info()
        lw_diaodu_info['lastweek_diaodu_subsidy'] = lw_diaodu_info['last_week_subsidy'].astype('float')
        lw_diaodu_info['lastweek_diaodu_gmv'] = lw_diaodu_info['lastweek_diaodu_gmv'].astype('float')
        # lw_diaodu_info['lastweek_avg_diaodu_rate'] = lw_diaodu_info['lastweek_avg_diaodu_rate'].astype('float')
        print(lw_diaodu_info.head(100))
        lw_diaodu_info = lw_diaodu_info.merge(last_week_idx, how='left', on='tdate')
        daily_optimal_result = daily_optimal_result.merge(this_week_idx, how='left', on='pred_date')
        daily_optimal_result = pd.merge(daily_optimal_result, lw_diaodu_info, how='left', on=['city_id', 'dt_idx']) 
        print("####22#####temp_df.count",daily_optimal_result.shape[0])
        # daily_optimal_result.to_csv('temp_df.csv')
        return daily_optimal_result

    def save_hdfs(self, df):
        temp_df = df.copy()
        print("***********save df************",temp_df.head())
        temp_df = temp_df.astype({'total_gmv': 'float', 'ori_c_budget': 'float',
                                'new_c_budget': 'float', 'ori_b_budget': 'float',
                                'new_b_budget': 'float', 'new_c_budget_rate': 'float', 
                                'new_diaodu_budget_rate': 'float','cur_diaodu_subsidy': 'float','succ_rate':'float',
                                'new_b_budget_rate': 'float','dgmv':'float','roi':'float'})
        result = []
        st_end_time_set = set(self.params['st_end_times'])
        for city_id in self.params['city_list'].split(','):
            city_id = int(city_id)
            city_df = temp_df.query('city_id == {}'.format(city_id))
            if ((len(city_df) == 0) ): # 修改城市时，一定注意 
                c_amount = 0.0
                b_amount = 0.0
                ori_b_budget = 0.0
                ori_c_budget = 0.0
                ori_acc_budget = 0.0
                c_rate = 0.0
                b_rate = 0.0
                ori_acc_rate = 0.0
                cur_diaodu_subsidy = 0.0
                new_diaodu_budget_rate = 0.0
                succ_rate = 0.0
                gmv = 1.0
                dgmv = 0.0
                roi = 0.0
                pangu_stg_detail = json.dumps([])
                acc_card_stg_detail = json.dumps([])
                lastweek_diaodu_subsidy = 0.0
                lastweek_diaodu_gmv = 0.0
                weekly_min_diaodu_rate = 0.0
                for cur_day in st_end_time_set:
                    result.append([cur_day,city_id,ori_c_budget,c_amount,c_rate,
                    ori_b_budget,b_amount,b_rate,cur_diaodu_subsidy,
                    new_diaodu_budget_rate,succ_rate,gmv,dgmv,roi,pangu_stg_detail,
                    acc_card_stg_detail,ori_acc_budget,ori_acc_rate, lastweek_diaodu_subsidy, 
                    lastweek_diaodu_gmv, weekly_min_diaodu_rate])   
            else:

                city_df['weekly_min_diaodu_rate'] = city_df['subsidy_money_lowerbound'].sum()/city_df['total_gmv'].sum()
                for _,row in city_df.iterrows():
                    result.append([row.pred_date,int(row.city_id),row.ori_c_budget,row.new_c_budget,row.new_c_budget_rate,row.ori_b_budget,row.new_b_budget,
                                row.new_b_budget_rate,row.cur_diaodu_subsidy,row.new_diaodu_budget_rate,
                                row.succ_rate,row.total_gmv,row.dgmv,row.roi,row.pangu_stg_detail,row.acc_card_stg_detail,row.ori_acc_budget,row.ori_acc_rate, 
                                row.lastweek_diaodu_subsidy, row.lastweek_diaodu_gmv, row.weekly_min_diaodu_rate])  # 加速卡添加'pangu_stg_detail','acc_card_stg_detail'
        temp_data = pd.DataFrame(result,columns=['pred_date','city_id','ori_c_budget','new_c_budget','new_c_budget_rate','ori_b_budget','new_b_budget','new_b_budget_rate',
                                'cur_diaodu_subsidy','new_diaodu_budget_rate','succ_rate','total_gmv','dgmv','roi','pangu_stg_detail','acc_card_stg_detail','ori_acc_budget',
                                'ori_acc_rate','lastweek_diaodu_subsidy', 'lastweek_diaodu_gmv', 'weekly_min_diaodu_rate'])
        print('********check data**********\n', temp_data)
        # temp_data.to_csv('temp_data.csv')
        # import pdb; pdb.set_trace()
        self.hc.createDataFrame(temp_data).repartition(1).registerTempTable('pred_table')
        self.hc.sql('''
            insert overwrite table bigdata_driver_ecosys_test.b_c_budget_allocate_collider_tmp2_diaodu partition (dt='{PRED_DATE}',mode='{trace_id}')
                select 
                pred_date,
                city_id,
                ori_c_budget,
                new_c_budget,
                new_c_budget_rate,
                ori_b_budget,
                new_b_budget,
                new_b_budget_rate,
                cur_diaodu_subsidy,
                new_diaodu_budget_rate,
                total_gmv,
                succ_rate,
                dgmv,
                roi,
                pangu_stg_detail,
                acc_card_stg_detail,
                ori_acc_budget,
                ori_acc_rate,
                lastweek_diaodu_subsidy,
                lastweek_diaodu_gmv,
                weekly_min_diaodu_rate,
                0.0 is_online,
                0.0 etc
            from pred_table
        '''.format(PRED_DATE=self.params['cur_day'],trace_id=self._stg_param['trace_id']))
        print('Saved to table: bigdata_driver_ecosys_test.b_c_budget_allocate_collider_tmp2_diaodu')
        return
        
    def extract_json_format(self, df):
        temp_df = df.copy()
        temp_df = temp_df.astype({'total_gmv': 'float', 'ori_c_budget': 'float',
                                  'new_c_budget': 'float', 'ori_b_budget': 'float',
                                  'new_b_budget': 'float', 'new_c_budget_rate': 'float', 'new_b_budget_rate': 'float',
                                  'new_diaodu_budget_rate': 'float','cur_diaodu_subsidy': 'float',
                                  'ori_acc_budget':'float','ori_acc_rate':'float', 'subsidy_money_lowerbound':'float'})

        temp_df['t_gmv'] = temp_df['total_gmv'].apply(
            lambda x: 1.0 if x == 0.0 else x * MONEY_UNIT)
        temp_df['ori_b_budget'] = temp_df['ori_b_budget'] * MONEY_UNIT
        temp_df['ori_acc_budget'] = temp_df['ori_acc_budget'] * MONEY_UNIT
        temp_df['ori_c_budget'] = temp_df['ori_c_budget'] * MONEY_UNIT
        temp_df['c_subsidy'] = temp_df['new_c_budget'] * MONEY_UNIT
        temp_df['b_subsidy'] = temp_df['new_b_budget'] * MONEY_UNIT 
        temp_df['c_rate'] = temp_df['new_c_budget_rate'] * MONEY_UNIT
        temp_df['b_rate'] = temp_df['new_b_budget_rate'] * MONEY_UNIT
        temp_df['acc_rate'] = temp_df['ori_acc_rate'] * MONEY_UNIT
        temp_df['new_diaodu_budget_rate'] = temp_df['new_diaodu_budget_rate'] * MONEY_UNIT
        temp_df['cur_diaodu_subsidy'] = temp_df['cur_diaodu_subsidy'] * MONEY_UNIT
        temp_df['lastweek_diaodu_subsidy'] = temp_df['lastweek_diaodu_subsidy']*MONEY_UNIT
        temp_df['lastweek_diaodu_gmv'] = temp_df['lastweek_diaodu_gmv']*MONEY_UNIT
        temp_df['subsidy_money_lowerbound'] = temp_df['subsidy_money_lowerbound']*MONEY_UNIT
        reslist = []


        total_b_money = 0.0
        total_c_money = 0.0
        total_diaodu_money = 0.0
        st_end_time_set = set(self.params['st_end_times'])
        print('total_time_scopes',st_end_time_set)
        for city_id in self._stg_param['city_list']:
            day_c_day_subsidy_dict = {}
            day_b_day_subsidy_dict = {}
            day_acc_day_subsidy_dict = {}
            day_diaodu_day_subsidy_dict = {}
            day_gmv_dict = {}
            pangu_stg_detail_dict = {}
            acc_stg_detail_dict = {}
            diaodu_ext_data_dict = {}

            city_df = temp_df.query('city_id == {}'.format(city_id))
            if (len(city_df) == 0) :  # 调整城市list时一定要check ！！！！！
                t_total_gmv = 1.0 * len(st_end_time_set)
                c_predict_rate = 0.0
                c_amount = 0.0
                b_predict_rate = 0.0
                b_amount = 0.0
                ori_b_budget = 0.0
                ori_c_budget = 0.0
                ori_acc_budget = 0.0
                ori_acc_rate = 0.0
                cur_diaodu_subsidy = 0.0
                new_diaodu_budget_rate = 0.0
                lastweek_diaodu_subsidy = 0.0
                lastweek_diaodu_gmv = 0.0
                diaodu_min_rate = 0.0
                diaodu_ext_data_dict['lastweek_subsidy'] = lastweek_diaodu_subsidy
                diaodu_ext_data_dict['last_gmv'] = lastweek_diaodu_gmv
                diaodu_ext_data_dict['min_rate'] = diaodu_min_rate
                for cur_day in st_end_time_set:
                    day_c_day_subsidy_dict[cur_day] = 0.0
                    day_b_day_subsidy_dict[cur_day] = 0.0
                    day_diaodu_day_subsidy_dict[cur_day] = 0.0
                    day_gmv_dict[cur_day] = 1.0
            else:
                t_total_gmv = city_df['t_gmv'].sum()
                c_predict_rate = city_df['c_subsidy'].sum() / city_df['t_gmv'].sum()
                c_amount = city_df['c_subsidy'].sum()
                b_predict_rate = city_df['b_subsidy'].sum() / city_df['t_gmv'].sum()
                b_amount = city_df['b_subsidy'].sum()  # 修改 
                new_diaodu_budget_rate = city_df['cur_diaodu_subsidy'].sum() / city_df['t_gmv'].sum()
                cur_diaodu_subsidy = city_df['cur_diaodu_subsidy'].sum()
                lastweek_diaodu_subsidy = city_df['lastweek_diaodu_subsidy'].sum()
                lastweek_diaodu_gmv = city_df['lastweek_diaodu_gmv'].sum()
                diaodu_min_rate = city_df['subsidy_money_lowerbound'].sum()/city_df['t_gmv'].sum()
                ori_b_budget = city_df['ori_b_budget'].sum()
                ori_acc_budget = city_df['ori_acc_budget'].sum() 
                ori_acc_rate = city_df['ori_acc_rate'].sum() 
                ori_c_budget = city_df['ori_c_budget'].sum()
                diaodu_ext_data_dict['lastweek_subsidy'] = lastweek_diaodu_subsidy
                diaodu_ext_data_dict['last_gmv'] = lastweek_diaodu_gmv
                diaodu_ext_data_dict['min_rate'] = diaodu_min_rate*100 if cur_diaodu_subsidy != 0 else 0.0
                for _,row in city_df.iterrows():
                    day_pred_date = row.pred_date
                    if row.c_subsidy >= 0:
                        day_c_day_subsidy_dict[day_pred_date] = row.c_subsidy
                    else:
                        day_c_day_subsidy_dict[day_pred_date] = 0.0
                    if row.b_subsidy > 0:
                        day_b_day_subsidy_dict[day_pred_date] = row.b_subsidy
                        if len(row.pangu_stg_detail) > 2 :
                            pangu_stg_detail_dict[day_pred_date] = json.loads(row.pangu_stg_detail)
                    else:
                        day_b_day_subsidy_dict[day_pred_date] = 0.0
                    if row.cur_diaodu_subsidy >= 0:
                        day_diaodu_day_subsidy_dict[day_pred_date] = row.cur_diaodu_subsidy
                    else:
                        day_diaodu_day_subsidy_dict[day_pred_date] = 0.0
                    if row.t_gmv >= 0:
                        day_gmv_dict[day_pred_date] = row.t_gmv
                    else:
                        day_gmv_dict[day_pred_date] = 0.0
                    if row.ori_acc_budget > 0:
                        day_acc_day_subsidy_dict[day_pred_date] = row.ori_acc_budget
                        if len(row.acc_card_stg_detail) > 2 :
                            acc_stg_detail_dict[day_pred_date] = json.loads(row.acc_card_stg_detail)
                    else:
                        day_acc_day_subsidy_dict[day_pred_date] = 0.0

            c_json = {
                "city_id": int(city_id),
                "predict_rate": c_predict_rate,
                "gmv": t_total_gmv,
                "amount": c_amount,
                "amount_type": "headquarters_budget",
                "caller": "dape-aps",
                "product_line": "kuaiche",
                "rec_detail": {
                    "ori_c": ori_c_budget,
                    "balance_c": c_amount-ori_c_budget,
                    "c_detail":day_c_day_subsidy_dict,
                    "gmv_detail":day_gmv_dict
                }
            }
            b_json = {
                "city_id": int(city_id),
                "predict_rate": b_predict_rate,
                "amount": b_amount,
                "gmv": t_total_gmv,
                "amount_type": "headquarters_budget",
                "caller": "b",
                "product_line": "kuaiche",
                "rec_detail": {
                    "ori_b": ori_b_budget,
                    "balance_b": b_amount-ori_b_budget,
                    "b_detail":day_b_day_subsidy_dict,
                    "gmv_detail":day_gmv_dict,
                    "stg_detail":pangu_stg_detail_dict
                }
            } if len(pangu_stg_detail_dict)!= 0 else {
                "city_id": int(city_id),
                "predict_rate": b_predict_rate,
                "amount": b_amount,
                "gmv": t_total_gmv,
                "amount_type": "headquarters_budget",
                "caller": "b",
                "product_line": "kuaiche",
                "rec_detail": {
                    "ori_b": ori_b_budget,
                    "balance_b": b_amount-ori_b_budget,
                    "b_detail":day_b_day_subsidy_dict,
                    "gmv_detail":day_gmv_dict
                }
            }
            diaodu_json = {
                "city_id": int(city_id),
                "predict_rate": new_diaodu_budget_rate,
                "amount": cur_diaodu_subsidy,
                "gmv": t_total_gmv,
                "amount_type": "headquarters_budget",
                "caller": "dispatch_b",
                "product_line": "kuaiche",
                "rec_detail": {
                    #"ori_dispatch_b": cur_diaodu_subsidy,
                    "ori_dispatch_b": 0.0,
                    "balance_dispatch_b": cur_diaodu_subsidy,
                    "dispatch_b_detail":day_diaodu_day_subsidy_dict,
                    "gmv_detail":day_gmv_dict,
                    "ext_data":json.dumps(diaodu_ext_data_dict)
                }
            } 

            if len(acc_stg_detail_dict)!= 0:
                acc_json = {
                    "city_id": int(city_id),
                    "predict_rate": ori_acc_rate,
                    "amount": ori_acc_budget,
                    "gmv": t_total_gmv,
                    "amount_type": "headquarters_budget",
                    "caller": "accelerate_card",
                    "product_line": "kuaiche",
                    "rec_detail": {
                        "ori_accelerate_card": ori_acc_budget,
                        "accelerate_card_detail":day_acc_day_subsidy_dict,
                        "gmv_detail":day_gmv_dict,
                        "stg_detail":acc_stg_detail_dict
                    }
                }
            else:
                acc_json = ""
            
            total_c_money += c_amount
            total_b_money += b_amount
            total_diaodu_money += cur_diaodu_subsidy           
            reslist.append(c_json)
            reslist.append(b_json)
            reslist.append(diaodu_json)
            if acc_json!= "":
                reslist.append(acc_json)
        print('call back：')
        print('c money: {}, b money: {}, diaodu money : {}'.format(total_c_money, total_b_money, total_diaodu_money))
        return reslist

    def generate_ouput_data(self):
        # 找线性规划的最优解

        df = self.find_optimal_combination()
        # df.to_csv('temp_df.csv')
        # 对结果保存hdfs
        # 转化为json，用于网络通信。
        self.save_hdfs(df)
        # df.to_csv("df_gallin_test.csv")
        # df = pd.read_csv('df_gallin_test.csv')

        reslist = self.extract_json_format(df)
        order_id = self._stg_param['order_id']
        operator = self._stg_param['operator']
        resp = {
            "errno": 0,
            "errmsg": "",
            "city_budget": json.dumps({"city_budget_list": reslist}),
            "order_id": order_id,
            "operator": operator
        }
        return resp
    def generate_b_gap_result(self):
        '''
        结合线性规划，根据弹性生成B端填坑的最优化组合，优化成交异常率
        :return:
        '''
        # 参数

if __name__ == '__main__':
    config = load_conf("./conf/job_config.json")
    stg_param = json.loads(sys.argv[1])
    # stg = Strategy(config, stg_param )
    # temp_res = stg.generate()
    # print(stg_param)
    for key in stg_param.keys():
        print(key, stg_param[key])
    print(stg_param)
