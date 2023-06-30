#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2021 Didiglobal.com. All Rights Reserved
#

"""
新智能运营系统t+1预算分配模块
Authors: jufei@didichuxing.com
Date:   2022/3/25 10:22 上午
"""

import json
import requests
import argparse
import pandas as pd
import os
import sys
import tempfile
import traceback
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from utils import execute_hive_sql, get_last_partition, send_message, validate_tasks, validate_input, call_back, \
    gmv_limit, upload_to_hive

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--param', default='')
args = arg_parser.parse_args()

PRED_TABLE = 'prod_smt_dw.offline_gongxu_pred_minute30_result'
DIAG_TABLE = 'prod_smt_dw.smt_budget_diagnosis_info_v2'
FENCE_RATE_TABLE = 'prod_smt_dw.smt_fence_gmv_ratio'
# TODO
CALL_BACK_URL = 'generalallocation/stgcallback'
GMV_URL = 'queryengine/predictvalue'

pd.set_option('expand_frame_repr', True)
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_columns', 50)

DELTA_FOC = [
    0.037, 0.046, 0.055, 0.065, 0.074, 0.083, 0.091, 0.1, 0.108, 0.116, 0.123, 0.13, 0.137, 0.143, 0.149, 0.155, 0.16,
    0.165, 0.169, 0.174, 0.177, 0.181, 0.184, 0.187, 0.189, 0.191, 0.193, float('inf')
]
B_RATE = [
    0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 0.05, 0.055, 0.06, 0.065, 0.07, 0.075, 0.08, 0.085, 0.09, 0.095, 0.1, 0.105,
    0.11, 0.115, 0.12, 0.125, 0.13, 0.135, 0.14, 0.145, 0.15, 0.15
]

DATA_TYPE = {
    'day': 'day_predict:gmv',
    'hour': 'hour_predict:gmv',
    'half_hour': 'hour_predict:gmv'
}

STRATEGY_TYPE_DICT = {
    "0.72": "fixed_threshold_72",
    "0.73": "fixed_threshold_73",
    "0.74": "fixed_threshold_74",
    "0.75": "fixed_threshold_75",
    "0.76": "fixed_threshold_76",
    "0.85": "exp_fixed_threshold_85",
    "0.86": "exp_fixed_threshold_86",
    "0.87": "exp_fixed_threshold_87",
    "0.88": "exp_fixed_threshold_88",
    "0.89": "exp_fixed_threshold_89"
}

PRODUCT_ID = {
    'objective_exp_openapi_pp': '2_obj',
    'exp_finish_order_pp': '2_exp'
}


def get_argument_parse(argument):
    """
    入参校验，解析
    @param argument:
    @return: param={"valid_cities":[2,3,5], 'start_date':'2022-03-23', 'strategy_type': 'fixed_threshold_72',
    'budget': 200000,
    "artificial_startegy": DataFrame,
    "b_ratio_limit":DataFrame,
    "cr_threshold": 0.72,
    }
    """
    argu = validate_input(argument)
    param = {
        'order_id': argu['order_id'],
        'step_id': argu['step_id'],
        'valid_cities': [],
        'start_date': argu['step_start_date'],
        'gmv_dt': argu['rely_info']['gmv_dt'],
        'strategy_type': STRATEGY_TYPE_DICT[argu['rely_info']['cr']],
        'budget': argu['budget_limit'],
        'cr_threshold':  float(argu['rely_info']['cr']),
        'cr_dt':  argu['rely_info']['cr_dt'],
        'cr_type': argu['rely_info']['cr_type'],
        'is_online': argu['is_online']
    }
    # 获取cr_dt, check最近可用分区;
    # cr_dt_1 = get_last_partition(PRED_TABLE)
    # cr_dt_2 = get_last_partition(DIAG_TABLE)
    # param['cr_dt'] = min(cr_dt_1, cr_dt_2)
    # print("cr_dt:", param['cr_dt'])

    if argu['is_online'] == 'test':
        param['call_back_ip'] = "http://dape-driver-test.didichuxing.com/darwin/"
    elif argu['is_online'] == 'pre':
        param['call_back_ip'] = "http://10.88.151.15:20627/darwin/"
    elif argu['is_online'] == 'online':
        param['call_back_ip'] = "http://10.88.128.149:32234/darwin/"

    for city_info in argu['ext_data']['input_city_info']:
        if city_info['d'] == argu['step_start_date'] and city_info['pl'] == 'kuaiche':
            param['valid_cities'].extend(city_info['cis'])

    artificial_startegy = []
    for artifical in argu['ext_data']['artificial']:
        if artifical['date'] == argu['step_start_date']:
            artificial_startegy.append({
                'dt': artifical['date'],
                'city_id': artifical['city_id'],
                'fence_id': artifical['fence_id'],
                'start_time': artifical["time_range"][0],
                'end_time': artifical["time_range"][1],
                'daily_b_rate': artifical['daily_b_rate']
            })
    param['artificial_startegy'] = pd.DataFrame(artificial_startegy)

    b_ratio_limit = []
    for budget_limit in argu['ext_data']['budget_limit']:
        if budget_limit['date'] == argu['step_start_date']:
            b_ratio_limit.append(budget_limit)
    param['b_ratio_limit'] = pd.DataFrame(b_ratio_limit)
    print("param after deal:", param)
    return param


def split_city_into_fence_id(art_df, start_date):
    """

    @param art_df:
    @param start_date:
    @return:
    """
    cities = list(art_df.city_id.unique())
    sql = f"""
        select city_id, fence_id, minute30, fence_gmv, city_gmv
        from {FENCE_RATE_TABLE}
        where concat_ws('-', year, month, day) = date_sub('{start_date}', 7)
        and city_id in ({','.join(map(str, cities))})
    """
    df = execute_hive_sql(sql)
    cols = list(art_df.columns)
    art_df.drop(columns='fence_id', inplace=True)
    ret_df = pd.merge(art_df, df, how='left')
    index = (ret_df['start_index'] <= ret_df['minute30']) & (ret_df['end_index'] >= ret_df['minute30'])
    ret_df = ret_df[index].groupby(cols, as_index=False)[['fence_gmv', 'city_gmv']].sum()
    print(ret_df)
    ret_df['gmv_ratio'] = ret_df['fence_gmv']/ret_df['city_gmv']
    city_df = ret_df.groupby(['city_id', 'start_time', 'end_time'], as_index=False)['fence_gmv'].sum().rename(
        columns={'fence_gmv': 'total_fence_gmv'})
    ret_df = pd.merge(ret_df, city_df, how='left')
    ret_df['budget_ratio'] = ret_df['fence_gmv']/ret_df['total_fence_gmv']
    return ret_df


def get_pred_cr(art_df, start_date, cr_dt, cr_type):
    """

    @param art_df:
    @param start_date:
    @param cr_dt:
    @param cr_type:
    @return:
    """
    cities = list(art_df.city_id.unique())
    product_id = PRODUCT_ID[cr_type]
    sql = f"""
            select city_id, fence_id, minute30, cr as cr_minute30
            from {PRED_TABLE}
            where dt = '{cr_dt}'
                and product_id = '{product_id}'
                and version = 'online'
                and result_date = '{start_date}'
                and city_id in ({','.join(map(str, cities))})
        """
    df = execute_hive_sql(sql)
    df['cr_minute30'] = df['cr_minute30'].astype('float')
    print(df)
    ret_df = pd.merge(art_df, df, how='left')
    cols = list(art_df.columns)
    index = (ret_df['start_index'] <= ret_df['minute30']) & (ret_df['end_index'] >= ret_df['minute30'])
    if ret_df[index].cr_minute30.isna().any():
        print("cr minute30 is missing")
    ret_df = ret_df[index].groupby(cols, as_index=False)['cr_minute30'].mean()
    return ret_df.rename(columns={'cr_minute30': 'cr'})


def get_url_pred_cr(art_df, param, data_type=None, pc_list=[]):
    """
    @param art_df:
    @param param:
    @param data_type:
    @param pc_list:
    @return:
    """
    if param['cr_type'] == "objective_exp_openapi_pp":
        data_type = 'hour_predict:objective_exp_openapi_pp'
        pc_list = ['tmp_110100_110400_110800_110103']
    elif param['cr_type'] == "exp_finish_order_pp":
        data_type = 'hour_predict:exp_finish_order_pp'
        pc_list = ['110100_110400_110800_110103']
    city_list = list(art_df[art_df.fence_id == -1].city_id.unique()) + list(
        art_df[art_df.fence_id != -1].fence_id.unique())
    fence_dict = dict(zip(art_df[art_df.fence_id != -1].fence_id, art_df[art_df.fence_id != -1].city_id))
    data = {
        'dt': param['cr_dt'],
        'data_type': data_type,
        'st_date_list': [param['start_date']],
        'pc_list': pc_list,
        'city_list': city_list
    }
    print(data)
    result = requests.post(param['call_back_ip'] + GMV_URL, data, timeout=5)
    result = json.loads(result.text)
    cr_list = []
    for cr_data in result.get('data').get('data'):
        city = int(cr_data['city_id'])
        if city < 500:
            cr_list.append({
                'city_id': city,
                'fence_id': -1,
                'minute30': int(cr_data['hour']),
                'cr_minute30': cr_data['key_value']
            })
        else:
            cr_list.append({
                'city_id': fence_dict[city],
                'fence_id': city,
                'minute30': int(cr_data['hour']),
                'cr_minute30': cr_data['key_value']
            })
    df = pd.DataFrame(cr_list)
    print(df)
    df['cr_minute30'] = df['cr_minute30'].astype('float')
    ret_df = pd.merge(art_df, df, how='left')
    cols = list(art_df.columns)
    index = (ret_df['start_index'] <= ret_df['minute30']) & (ret_df['end_index'] >= ret_df['minute30'])
    if ret_df[index].cr_minute30.isna().any():
        print("cr minute30 is missing")
    ret_df = ret_df[index].groupby(cols, as_index=False)['cr_minute30'].mean()
    return ret_df.rename(columns={'cr_minute30': 'cr'})


def get_minute30_index(df, date_time):
    """

    @param df:
    @param date_time:
    @return:
    """
    hour, minute, _ = df[date_time].strip().split(' ')[-1].split(':')
    return int(hour) * 2 + round(int(minute) / 30)


def get_pred_gmv(df, param, data_type, pc_list=['110100_110400_110800_110103']):
    """
    风险点： 半小时口径，还是小时口径，和data_type无关，和pc_list有关
    @param df:
    @param param:
    @param data_type:
    @param pc_list:
    @return:
    """
    data = {
        'dt': param['gmv_dt'],
        'data_type': DATA_TYPE[data_type],
        'st_date_list': [param['start_date']],
        'pc_list': pc_list,
        'city_list': [int(x) for x in df.city_id.unique()]
    }
    print(data)
    result = requests.post(param['call_back_ip']+GMV_URL, data, timeout=5)
    result = json.loads(result.text)
    # print('gmv result:', result)
    gmv_list = []
    for gmv_data in result.get('data').get('data'):
        city = gmv_data['city_id']
        if data_type == "day":
            gmv_list.append({
                'city_id': city,
                'minute30': 0,
                'gmv': gmv_data.get('estimate_value')
            })
        elif data_type == "hour":
            gmv_list.append({
                'city_id': city,
                'minute30': int(gmv_data['hour'])*2,
                'gmv': gmv_data['key_value']/2
            })
            gmv_list.append({
                'city_id': city,
                'minute30': int(gmv_data['hour'])*2+1,
                'gmv': gmv_data['key_value']/2
            })
        elif data_type == "half_hour":
            gmv_list.append({
                'city_id': city,
                'minute30': int(gmv_data['hour']),
                'gmv': gmv_data['key_value']
            })

    gmv_df = pd.DataFrame(gmv_list).drop_duplicates()
    if gmv_df.shape[0] == 0:
        raise Exception('can not get the gmv from api')
    cols = list(df.columns)
    ret_df = pd.merge(df, gmv_df, how='left')
    if data_type == 'day':
        return ret_df.rename(columns={'gmv': 'total_gmv'}).drop(columns=['minute30'])
    index = (ret_df['start_index'] <= ret_df['minute30']) & (ret_df['end_index'] >= ret_df['minute30'])
    ret_df = ret_df[index].groupby(cols, as_index=False)['gmv'].sum()
    print(df)
    print(gmv_df)
    print(ret_df)
    ret_df['gmv'] = ret_df['gmv'] * ret_df['gmv_ratio']
    return ret_df


def get_task(param):
    """
    根据对应阈值获取活动， 屏蔽人工
    @param param:
    @return: city_id, fence_id, start_time,end_time, cr, gmv
    """
    valid_cities = param['valid_cities']
    start_date = param['start_date']
    cr_dt = param['cr_dt']
    strategy_type = param['strategy_type']
    sql = f"""
        select 
            a.dt dt, 
            a.city_id city_id, 
            a.fence_id fence_id, 
            a.start_time start_time, 
            a.end_time end_time, 
            a.cr cr, 
            a.cr_thres cr_thres, 
            if(a.gmv_ratio>1, 1, a.gmv_ratio) gmv_ratio, 
            0 as remove_tag
            -- sum(nvl(if(a.end_time>b.start_datetime and b.end_datetime>a.start_time, 1, 0), 0)) as remove_tag
       from (
        select 
            result_date as dt,
            city_id,
            fence_id,
            start_time,
            end_time,
            cr,
            cr_bar as cr_thres,
            gmv_ratio
        from {DIAG_TABLE}
        where concat_ws('-', year, month, day) = '{cr_dt}'
        and result_date = '{start_date}'
        and strategy_type = '{strategy_type}'  
        and tool = '智能盘古'
        and cr < cr_bar
       ) a
    """
    #    left join(
    #     select
    #         activity_info.dt dt,
    #         cast(activity_info.city_id as int) city_id,
    #         cast(if(job_info.strategy like '%dape%'  or job_info.strategy like '%pangu_exp%'
    #         , tre.fence_id
    #         , if(job_info.fence_id=0, -1, job_info.fence_id)
    #         ) as int) as fence_id,
    #         substr(start_datetime, 12) as start_datetime,
    #         substr(end_datetime, 12) as end_datetime
    #         -- hour(start_datetime)*2+if(minute(start_datetime)=30, 1, 0) as start_index,
    #         -- hour(end_datetime)*2+if(minute(end_datetime)=30, 1, 0)-1 as end_index
    #     from
    #         (--活动下发日期
    #         select
    #             distinct job_id,
    #             city_id,
    #             city_name,
    #             tag_id,
    #             activity_id,
    #             substr(start_datetime, 1, 10) as dt,
    #             start_datetime,
    #             end_datetime
    #         from
    #             prod_smt_stg.ods_binlog_activity_info_whole
    #         where
    #             concat_ws('-',year,month,day) = '{cr_dt}' --最新分区，全量表
    #             and hour<=14
    #             and status = 4
    #             and job_type in (2,3,20)
    #             and substr(start_datetime, 1, 10) between '{start_date}' and '{start_date}' --活动开始时间
    #         ) activity_info
    #         join (
    #             select
    #                 distinct id,
    #                 city_id,
    #                 split(strategy,'#')[0] strategy, --修复bug
    #                 reverse(split(reverse(split(strategy,'#')[0]), '_')[0]) fence_id,
    #                 batch_exec_detail_id
    #             from
    #                 prod_smt_stg.ods_binlog_job_info_whole
    #             where
    #                 concat_ws('-',year,month,day) = '{cr_dt}'  --最新分区，全量表
    #                 and job_type in (2,3,20) --新增战区实验
    #                 and status = 3
    #         ) job_info on activity_info.job_id = job_info.id
    #         left join (
    #             select
    #                 distinct strategy_version, canvas_id, fence_id
    #             from
    #                 mp_data.dwd_trip_mp_mkt_realtime_pangu_driver_result_hi
    #             where
    #                 concat_ws('-', year,month,day) between date_sub('{cr_dt}', 15)  and date_add('{cr_dt}',1)
    #                 and incentive_type in ('pangu', 'pangu_exp') --t+7,t+1,战区实验
    #         ) tre on job_info.strategy=tre.strategy_version and job_info.batch_exec_detail_id=tre.canvas_id
    #     ) b
    #     on a.dt = b.dt
    #         and a.city_id=b.city_id
    #         and a.fence_id=b.fence_id
    #     group by a.dt, a.city_id, a.fence_id, a.start_time, a.end_time, a.cr, a.cr_thres, a.gmv_ratio
    # """
    df = execute_hive_sql(sql)
    print(df)
    df = df[df.city_id.isin(valid_cities)]
    if df[df.remove_tag != 0].shape[0] > 0:
        print(df[df.remove_tag != 0])
        send_message("B端T+1预算分配", f"warning： {','.join([str(x) for x in list(df[df.remove_tag != 0].city_id.unique())])} 存在已下发活动与诊断活动冲突")
    df = df[df.remove_tag == 0]
    if df.shape[0] > 0:
        df['start_index'] = df.apply(lambda x: get_minute30_index(x, 'start_time'), axis=1)
        df['end_index'] = df.apply(lambda x: get_minute30_index(x, 'end_time') - 1, axis=1)
        df = get_pred_gmv(df, param, 'day')
        df = get_pred_gmv(df, param, 'half_hour')

    # 人工策略
    art_df = param['artificial_startegy']
    if art_df.shape[0] > 0:
        art_df['start_index'] = art_df.apply(lambda x: get_minute30_index(x, 'start_time'), axis=1)
        art_df['end_index'] = art_df.apply(lambda x: get_minute30_index(x, 'end_time') - 1, axis=1)
        # 拆分到围栏
        art_df = split_city_into_fence_id(art_df, start_date)
        # cr补充
        if param['cr_type'] == 'objective_exp_openapi_pp':
            art_df = get_pred_cr(art_df, start_date, cr_dt, param['cr_type'])
        elif param['cr_type'] == 'exp_finish_order_pp':
            art_df = get_url_pred_cr(art_df, param)
        else:
            raise Exception(f'cr_type is Error, detail:{param["cr_type"]}')
        art_df['cr_thres'] = param['cr_threshold']
        # gmv获取及预算分配
        art_df = get_pred_gmv(art_df, param, 'day')
        art_df = get_pred_gmv(art_df, param, 'half_hour')
        art_df['budget'] = art_df['total_gmv'] * art_df['daily_b_rate'] * art_df['budget_ratio']

    # b补率限制
    b_ratio_df = param['b_ratio_limit']
    budget_limit = {}
    if b_ratio_df.shape[0] > 0:
        b_ratio_df = get_pred_gmv(b_ratio_df, param, 'day')
        b_ratio_df['budget'] = b_ratio_df['total_gmv'] * b_ratio_df['daily_b_rate_limit']
        print("b_ratio_df:", b_ratio_df[b_ratio_df.city_id<50])
        budget_limit = dict(zip(b_ratio_df['city_id'], b_ratio_df['budget']))
    for city_id in range(1, 500):
        if city_id not in budget_limit:
            budget_limit[city_id] = 100000000
    return df, art_df, budget_limit


def t_plus_1_alloc_tasks(param, raw_tasks, b_budget_limit):
    """
    预算分配
    @param param:
    @param raw_tasks:
    @param b_budget_limit:
    @return:
    """
    total_budget = param['latest_budget']

    # 得到最大的b补率
    def get_max_b_rate(r):
        # 非坑最多2pp
        if r.cr_thres < r.cr:
            return 0.02
        dur = datetime.strptime(r.end_time, "%H:%M:%S") - datetime.strptime(r.start_time, "%H:%M:%S")
        dur = dur.seconds / 3600
        if dur > 8:
            return 0.08
        return 0.12
    raw_tasks['max_brate'] = raw_tasks.apply(lambda r: get_max_b_rate(r), axis=1)

    # 得到合适的b补率
    def get_suit_b_rate(r):
        delta_cr = ((r.cr_thres - r.cr) / (r.cr + 0.000001))
        exp_brate = 0.02
        for i, val in enumerate(DELTA_FOC):
            if delta_cr <= val:
                exp_brate = B_RATE[i]
                break
        return min(exp_brate, r.max_brate)
    raw_tasks['suit_brate'] = raw_tasks.apply(lambda r: get_suit_b_rate(r), axis=1)
    # 得到最小的b补率
    def get_min_b_rate(r):
        # 非坑最多2pp
        dur = datetime.strptime(r.end_time, "%H:%M:%S") - datetime.strptime(r.start_time, "%H:%M:%S")
        dur = dur.seconds / 3600
        if dur <= 3:
            return 0.06
        return 0.02
    raw_tasks['min_brate'] = raw_tasks.apply(lambda r: get_min_b_rate(r), axis=1)

    raw_tasks["budget"] = 0
    raw_tasks = raw_tasks[raw_tasks.gmv >= 6000].sort_values('cr').reset_index(drop=True)
    print(raw_tasks)

    def execute_alloc(col, current_budget):
        b_rate_step = 0.01
        # base_b_rate = 0.02
        loop = True
        while loop:
            cnt = 0
            for i in range(raw_tasks.shape[0]):
                if current_budget > total_budget:
                    loop = False
                    break
                if raw_tasks.loc[i, "budget"] > raw_tasks.loc[i, "gmv"] * (raw_tasks.loc[i, col] - b_rate_step / 2):
                    cnt += 1
                    continue
                if raw_tasks.loc[i, "budget"] == 0:
                    budget = raw_tasks.loc[i, "gmv"] * raw_tasks.loc[i, "min_brate"]
                else:
                    budget = raw_tasks.loc[i, "gmv"] * b_rate_step
                if budget > b_budget_limit[raw_tasks.loc[i, "city_id"]]:
                    cnt += 1
                    continue
                current_budget += budget
                raw_tasks.loc[i, "budget"] += budget
                b_budget_limit[raw_tasks.loc[i, "city_id"]] -= budget
            if cnt == raw_tasks.shape[0]:
                loop = False
        return current_budget

    proposed_budget = 0
    need_budget = defaultdict(int)
    for _, line in raw_tasks.iterrows():
        need_budget[line['city_id']] += line['gmv']*max(line['suit_brate'], line['min_brate'])
    for key, val in need_budget.items():
        if val > b_budget_limit[key]:
            print("budget limit:", key, val, b_budget_limit[key])
        proposed_budget += min(val, b_budget_limit[key])
    print("need_budget:", need_budget)
    print("need_budget:", sum([need_budget[x] for x in need_budget]))
    print("proposed_budget:", proposed_budget)
    print(b_budget_limit)
    alloc_budget = execute_alloc('suit_brate', 0)
    print(alloc_budget)
    print(raw_tasks)
    alloc_budget = execute_alloc('max_brate', alloc_budget)
    print(alloc_budget)
    if alloc_budget < total_budget:
        print("failed to burn total_budget")
    print('raw_tasks:\n', raw_tasks)
    raw_tasks = raw_tasks[raw_tasks.budget > 100]
    ratio = raw_tasks.budget.sum() / total_budget
    print(f"ratio {ratio}, total_budget {total_budget}, real_budget {raw_tasks.budget.sum()}")
    if ratio > 1:
        raw_tasks.budget = raw_tasks.budget / ratio
        print(f"ratio {ratio} > 1, adjust budget to {total_budget}")
    raw_tasks.budget = raw_tasks.budget.astype(int)
    print(f"start to refresh tasks, refresh budget {raw_tasks.budget.sum():.2f}")
    # print(raw_tasks)
    return proposed_budget, raw_tasks[raw_tasks.budget > 100]


def generate_final_tasks(param, tasks, proposed_budget):
    """
    结果格式化
    @param param:
    @param tasks:
    @param proposed_budget:
    @return:
    """
    df_city = pd.DataFrame(param['valid_cities'], columns=['city_id'])
    if df_city.shape[0] > 0:
        df_city = get_pred_gmv(df_city, param, 'day')
    print(df_city)
    ret = {
        "rely_info": {
            "gmv_dt": param['gmv_dt'],
            "cr_dt": param['cr_dt'],
            "cr": str(round(param['cr_threshold'], 2)),
            "cr_type": param["cr_type"],
            "proposed_budget": proposed_budget
        },
        "version": ""
    }
    tasks = tasks.sort_values(['city_id', 'fence_id', 'start_time'])
    data = []
    act_cities = tasks.city_id.unique()
    cities = sorted(set(list(tasks.city_id.unique()) + param['valid_cities']))
    print(cities)
    for city_id in cities:
        city_data = {
            "city": int(city_id),
            "product_line": "kuaiche",
            "caller": "b",
            "stat_date": param["start_date"],
        }
        stg_detail = []
        amount = 0
        gmv = 0
        if city_id not in act_cities:
            city_data['stg_detail'] = []
            city_data['amount'] = 0.0
            city_data['gmv'] = gmv_limit(df_city.loc[df_city.city_id == city_id, 'total_gmv'].max())
            city_data['step_type'] = "B_T_1"
            data.append(city_data)
            continue
        for _, line in tasks[tasks.city_id == city_id].iterrows():
            stg_detail.append(
                {
                    "start_time": line["start_time"],
                    "end_time": line["end_time"],
                    "fence_id": -1 if line['fence_id'] < 500 else line['fence_id'],
                    "interval_cr": round(line['cr'], 4),
                    "pit_depth": round(line['cr_thres'] - line['cr'], 4),
                    "amount": round(float(line['budget']), 4),
                    "interval_gmv": gmv_limit(line['gmv'])
                }
            )
            amount += float(line['budget'])
            gmv = max(gmv, line['total_gmv'])
        city_data['stg_detail'] = stg_detail
        city_data['amount'] = round(amount, 4)
        city_data['gmv'] = gmv_limit(gmv)
        city_data['step_type'] = "B_T_1"
        data.append(city_data)
    ret['data'] = data
    result = {
        'order_id': param['order_id'],
        'step_id': param['step_id'],
        'version_info': json.dumps([ret])
    }
    return result


def main(param):
    """
    主函数
    @return:
    """

    raw_tasks, art_tasks, budget_limit = get_task(param)
    param['latest_budget'] = param['budget']
    if art_tasks.shape[0] > 0:
        param['latest_budget'] -= art_tasks['budget'].sum()
    if param['latest_budget'] < 0:
        raise Exception(
            f"""BUDGET ERROR, total budget {param['budget']} less than artifical budget {art_tasks.budget.sum()}""")

    cols = ['dt', 'city_id', 'fence_id', 'start_time', 'end_time', "cr", "cr_thres", "gmv", "budget", "total_gmv", "strategy_type"]
    tasks = pd.DataFrame([], columns=cols)
    proposed_budget = 0
    if raw_tasks.shape[0] > 0:
        proposed_budget, tasks = t_plus_1_alloc_tasks(param, raw_tasks, budget_limit)
        tasks['strategy_type'] = 'smart'
        tasks = tasks[cols]
    if art_tasks.shape[0] > 0:
        art_tasks['strategy_type'] = 'artificial'
        tasks = pd.concat([tasks, art_tasks[art_tasks.budget >= 100][cols]])
        proposed_budget += art_tasks[art_tasks.budget >= 100].budget.sum()
    print(tasks)
    # 结果校验
    err = validate_tasks(tasks)
    if err != "":
        raise Exception(f"预算格式非法：{err}")
    # 结果写表
    tasks['city_list'] = ','.join([str(x) for x in param['valid_cities']])
    if datetime.now().strftime('%Y-%m-%d') < param['start_date'] and param['is_online'] == 'online':
        upload_to_hive(
            tasks,
            columns=['city_id', 'fence_id', 'start_time', 'end_time', "cr", "cr_thres", "gmv", "budget", "total_gmv", "strategy_type", "city_list"],
            table='prod_smt_dw.smt_budget_alloc_t1',
            partition_date=param['start_date'],
            external_partition=f"product_id='{PRODUCT_ID[param['cr_type']]}'"
        )
    # 返回结果
    ret_data = generate_final_tasks(param, tasks, proposed_budget)
    print(ret_data)
    # if param['is_online'] == 'test':
    #     return None
    # 接口回调
    if not call_back(param['call_back_ip']+CALL_BACK_URL, ret_data):
        raise Exception(f"回调失败")


if __name__ == '__main__':
    exit_code = 0
    try:
        # 参数解析
        param = get_argument_parse(args.param)
        main(param)
        # todo修正
        if param['is_online'] != 'test':
            send_message("B端T+1预算分配", f"分配完成")
    except Exception as e:
        _, exc_value, exc_obj = sys.exc_info()
        print(f"exception:\n\t{exc_value}\n\ntraceback: \n")
        traceback.print_tb(exc_obj)
        # todo修正
        if param['is_online'] != 'test':
            send_message("B端T+1预算分配", f"分配任务发生异常，错误：{e}")
            exit_code = 1
            # 接口回调

            call_back(
                param['call_back_ip']+CALL_BACK_URL,
                data={
                    'err_no': 1,  'err_msg': e,
                    'order_id': param['order_id'],
                    'step_id': param['step_id'],
                    'version_info': ''
                }
            )

    exit(exit_code)
