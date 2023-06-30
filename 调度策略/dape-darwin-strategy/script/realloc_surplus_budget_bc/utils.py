#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import datetime
import calendar
import os
import logging
import json
import tempfile
import pandas as pd
import numpy as np
import requests
from ortools.sat.python import cp_model
from ortools.linear_solver import pywraplp
from minio import Minio
import random
from datetime import datetime

logger = logging.getLogger(__name__)
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.set_option('expand_frame_repr', False)
MONEY_UNIT = 100


def get_next_week_time(day_str):
    memory_day = datetime.datetime.strptime(day_str, "%Y-%m-%d")
    curday = datetime.datetime.strptime(day_str, "%Y-%m-%d")
    oneday = datetime.timedelta(days=1)
    sevenday = datetime.timedelta(days=7)
    m1 = calendar.MONDAY
    m2 = calendar.SUNDAY
    while (curday.weekday() != m1) or (memory_day == curday):
        curday += oneday
    nextMonday = curday.strftime('%Y-%m-%d')
    while curday.weekday() != m2:
        curday += oneday
    nextSunday = (curday + sevenday).strftime('%Y-%m-%d')
    return nextMonday, nextSunday

def get_after_14_day(day_str):
    curday = datetime.datetime.strptime(day_str, "%Y-%m-%d")
    oneday = datetime.timedelta(days=1)
    fourteenday = datetime.timedelta(days=14) 
    
    after_1_day = (curday + oneday).strftime('%Y-%m-%d')
    after_14_day = (curday + fourteenday).strftime('%Y-%m-%d')
    return after_1_day, after_14_day

def load_conf(conf_file):
    if not os.path.exists(conf_file):
        raise Exception('Failed to find config file: %s' % conf_file)
    with open(conf_file) as fconf:
        config = json.load(fconf)
        logger.info('config: %s' % config)
        return config


def execute_cmd(cmd):
    logger.info('executing command: %s' % cmd)
    ret = os.system(cmd)
    if ret != 0:
        raise Exception('Failed to execute command: %s' % cmd)


def execute_cmd_and_get_result(cmd):
    logger.info('executing command: %s' % cmd)
    with os.popen(cmd) as f:
        content = f.read()
    return content


def execute_hive_sql(sql):
    _, output = tempfile.mkstemp(prefix='smt_')
    cmd = 'hive --hiveconf mapreduce.job.queuename=root.dev_pricing_driver_prod -e "{sql}" > {output}'.format(
        sql=sql,
        output=output
    )
    print(cmd)
    os.system(cmd)
    return pd.read_csv(output, sep="\t")


def hdfs_path_exist(hdfs_path):
    cmd = 'hadoop fs -test -e %s' % hdfs_path
    try:
        execute_cmd(cmd)
    except:
        return False
    return True


def time_to_float_hour(time_str):
    parts = time_str.split(':')
    if len(parts) < 2:
        return -1
    ret = 2*float(parts[0])
    if float(parts[1]) >= 15 and float(parts[1]) <= 45:
        ret += 1
    elif float(parts[1]) > 45:
        ret += 2
    return ret

def rescale_b_gap_money(x):
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
        amount_gap = x.new_b_budget1 - sum_amount

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

def contraint_opt(pdf_1d, totalbudget, params):
    # pdf_1d = opti_data[['idx','city_id','pred_date','budget_c_gap','budget_b_gap','gmv','dgmv','total_cost','new_c_rate','time_segment_infos']]
    citylist = pdf_1d['city_id'].unique()
    nextMonday, nextSunday = params['c_start_time'], params['c_end_time']

    model = cp_model.CpModel()
    xs = []  # using one-hot
    dgmvs = []
    gmvs = []
    ttlcosts = []
    s_lists = []
    tp_all_gmv = 0
    tp_all_dgmv = 0

    output_index = []
    # for city_id, true_s in city2s_ttl.items():
    num_of_city_day = -1
    num_of_city_day_list = []
    for city_id in citylist:
        for cur_day in pd.date_range(nextMonday, nextSunday):
            cur_day = cur_day.strftime('%Y-%m-%d')
            # print("city_id: {}, cur_day: {}".format(city_id,cur_day))

            cdf = pdf_1d[(pdf_1d.city_id == city_id) &
                         (pdf_1d.pred_date == cur_day)]
            if(len(cdf) == 0):
                continue

            tp_all_gmv += cdf['gmv'].values[0]
            tp_all_dgmv += cdf['dgmv'].values[0]
            min_s = 0.009
            max_s = 0.08
            #cdf = pdf_1d[(pdf_1d.city_id == city_id) & (pdf_1d.hf_s >= min_s) & (pdf_1d.hf_s >= weiwen_sub)]
            s_list = cdf['idx'].values
            s_lists.append(s_list)
            xs.append(np.array([model.NewBoolVar(
                name='city_%d_day_%s_index_%d' % (city_id, cur_day, s)) for s in s_list]))
            dgmvs.append(cdf['dgmv'].values * 100.0)
            gmvs.append(cdf['gmv'].values)
            ttlcosts.append(cdf['total_cost'].values)
            num_of_city_day += 1
            num_of_city_day_list.append([city_id, cur_day, num_of_city_day])

    tp_all_cost = totalbudget
    s_tar_ave = tp_all_cost / tp_all_gmv
    tp_all_roi = tp_all_dgmv / tp_all_cost
    xs = np.array(xs)
    dgmvs = np.array(dgmvs)
    gmvs = np.array(gmvs)
    totalcosts = np.array(ttlcosts)
    s_lists_np = np.array(s_lists)

    for x in xs:  # x is onehot representation of subsidy
        model.Add(x.sum() == 1)

    total_cost = np.sum((xs[i] * (totalcosts[i]).astype(np.int32)).sum()
                        for i in range(num_of_city_day))
    total_dgmv = np.sum((xs[i] * dgmvs[i].astype(np.int32) *
                        gmvs[i].astype(np.int32)).sum() for i in range(num_of_city_day))
    total_budget = np.sum((xs[i] * (gmvs[i] * s_tar_ave).astype(np.int32)).sum()
                          for i in range(num_of_city_day))
    model.Add(100 * total_cost <= 100 * total_budget)
    model.Maximize(total_dgmv)
    # 4. output the result
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 480.0
    status = solver.Solve(model)

    if status == cp_model.OPTIMAL:
        gmv_di = []
        dgmv_di = []
        cost_di = []
        # for i, city_id in enumerate(city2s):
        for (pred_date, city_id, i) in num_of_city_day_list:

            for flag, s, dgmv, gmv, tcost in zip(xs[i], s_lists[i], dgmvs[i], gmvs[i], totalcosts[i]):
                if solver.Value(flag):
                    plans = pdf_1d.query("idx == %d" % s)
                    output_index.append(s)
                    dgmv_di.append(dgmv)
                    cost_di.append(tcost)
                    gmv_di.append(gmv)
                    # city2new_s[city_id][pred_date] = 'total cost plan idx %d , b_sub %.4f , c_sub %.4f'%(s, plans['budget_c_gap'], plans['budget_b_gap'])

        # print("[New]", city2new_s)
        all_gmv = np.sum(gmv_di)
        all_cost = np.sum(cost_di)
        ave_s = all_cost / all_gmv
        all_dgmv = np.sum(dgmv_di)
        all_roi = all_dgmv / all_cost
        print("Realloc: ave_s={}, gmv={}, dgmv={}, roi={} \n".format(
            ave_s, all_gmv, all_dgmv, all_roi))
        print("True_Pred: ave_s={}, gmv={}, dgmv={}, roi={} \n".format(
            s_tar_ave, tp_all_gmv, tp_all_dgmv, tp_all_roi))
        print('Pred roi grow={} \n'.format(all_roi / tp_all_roi - 1))

    choose_scheme = pdf_1d.iloc[output_index, :]
    cost_budget = choose_scheme['total_cost'].sum()
    print('total_cost:', cost_budget)
    return choose_scheme, cost_budget


def use_day_contraint_opt(opti_data, remain_budget, params):
    print('整数规划的金额：{}'.format(remain_budget))
    if remain_budget > 0.0:
        all_day_lists = pd.date_range(
            params['start_time'], params['end_time']).astype(str)
        num_of_day = len(all_day_lists)
        average_remain_budget = remain_budget / num_of_day
        print(num_of_day, average_remain_budget)
        every_day_costs = []
        every_day_choose_schemes = []
        for c_day in all_day_lists:
            print(c_day)
            params['c_start_time'] = c_day
            params['c_end_time'] = c_day
            choose_scheme_i, cost_budget = contraint_opt(
                opti_data, average_remain_budget, params)
            if cost_budget == 0.0:
                choose_scheme_i = opti_data.query(
                    '(budget_c_gap ==0.0) and (budget_b_gap == 0.0) and (pred_date == "{}")'.format(c_day))
            every_day_costs.append(cost_budget)
            every_day_choose_schemes.append(choose_scheme_i)
        params['margin_budget'] = remain_budget - np.sum(every_day_costs)
        choose_scheme = pd.concat(every_day_choose_schemes)
    else:
        choose_scheme = opti_data.query(
            '(budget_c_gap ==0.0) and (budget_b_gap == 0.0) ')
        params['margin_budget'] = remain_budget
    print('整数规划后剩余金额：{}'.format(params['margin_budget']))
    return choose_scheme


def linear_contraint(opti_data, total_budget, params, min_elem_weight = 0.000001):
    from datetime import datetime
    start_a = datetime.now()

    if total_budget <= 0.0:
        choose_scheme = opti_data.query(
            '(budget_c_gap ==0.0) and (budget_b_gap == 0.0) and (subsidy_money_lowerbound == cur_diaodu_subsidy)')
        return choose_scheme
    
    # 调度为b的25%, 同时去掉填坑已经占的比例
    holiday_weight = 0.3
    c_weight = params['c_weight']
    #add diaodu weight
    d_budget = max(min(params['d_weight'] * params['ori_total_budget'] - params['dt_min_spent'],total_budget),1000) # dd rest budget
    c_budget = max(min(params['c_weight'] * params['ori_total_budget'] - params['c_spent'],total_budget),1000)
    b_budget = max(min(params['b_weight'] * params['ori_total_budget'] - params['b_spent'] - params['dt_min_spent'],total_budget),1000)

    print('total_budget:{}'.format(total_budget))
    print('b&c_budget:{}, d_budget:{}'.format( total_budget-d_budget, d_budget))
    
    opti_data['budget_c_gap'] = opti_data['budget_c_gap'].astype(float)
    opti_data['budget_b_gap'] = opti_data['budget_b_gap'].astype(float)
    opti_data['dt_gap'] = opti_data['dt_gap'].astype(float)

    x = {}
    solver = pywraplp.Solver.CreateSolver('GLOP')
    solver.max_time_in_seconds = 240.0
    for i in range(len(opti_data)):
        x[i] = solver.NumVar(0.0, 1, 'x[%i]' % (i))

    solver.Add(solver.Sum([x[i] * opti_data['budget_c_gap'].iloc[i]+  x[i] * opti_data['budget_b_gap'].iloc[i] 
                           for i in range(0, len(x))]) <= total_budget - d_budget)
    solver.Add(solver.Sum([x[i] * opti_data['dt_gap'].iloc[i]
               for i in range(0, len(x))]) <= d_budget * 1.20)

    solver.Add(solver.Sum([x[i] * opti_data['budget_c_gap'].iloc[i]
               for i in range(0, len(x))]) >= 0)
    solver.Add(solver.Sum([x[i] * opti_data['budget_b_gap'].iloc[i]
               for i in range(0, len(x))]) >= 0)
    solver.Add(solver.Sum([x[i] * opti_data['dt_gap'].iloc[i]
               for i in range(0, len(x))]) >= 0)


    for city in list(set(opti_data['city_id'])):
        for day in pd.date_range(params['start_time'], params['end_time']).astype(str):
            citydf = opti_data.query("city_id == %d & pred_date == '%s'" % (city, day))
            idxlist = []
            if len(citydf) > 0 :
                for j in range(0, len(citydf)):
                    idxlist.append(citydf.iloc[j]['idx'])
                solver.Add(solver.Sum([x[idxlist[i]] for i in range(0, len(idxlist))]) <= 1)
                solver.Add(0.01 <= solver.Sum([x[idxlist[i]] for i in range(0, len(idxlist))]))


    idxlist_2 = []
    total_day_num = 1
    holiday_num = 1
    for day in pd.date_range(params['start_time'], params['end_time']).astype(str):
        total_day_num += 1
        if day in params['holiday_set']:
            holiday_num += 1
            citydf = opti_data.query("pred_date == '%s'" % (day))
            for j in range(0, len(citydf)):
                idxlist_2.append(citydf.iloc[j]['idx'])

    if (len(idxlist_2) > 0):
        solver.Add(solver.Sum([x[idxlist_2[i]] * opti_data['budget_c_gap'].iloc[idxlist_2[i]]
                               for i in range(0, len(idxlist_2))]) <=
                   holiday_weight * holiday_num / total_day_num * c_weight * total_budget)

    print('Number of variables =', solver.NumVariables())
    print('Number of constraints =', solver.NumConstraints())
    solver.Maximize(solver.Sum([x[i] * opti_data['dgmv'].iloc[i]
                    * opti_data['total_gmv'].iloc[i] for i in range(0, len(x))]))
    status = solver.Solve()

    reslist = []
    start_b = datetime.now()
    print("规划总计花费时长(s): ）",(start_b - start_a).seconds)
    if status == pywraplp.Solver.OPTIMAL:
        print('Solution:')
        print('Objective value =', solver.Objective().Value())

        tempdict = {}
        for i in range(0, len(x)):
            cur_val = x[i].solution_value()
            if cur_val > 0.0:
                tempkey = '%d_%s' % (opti_data['city_id'].iloc[i], opti_data['pred_date'].iloc[i])
                if not tempkey in tempdict:
                    tempdict[tempkey] = (cur_val,i)
                else:    
                    his_max_val,his_idx = tempdict[tempkey]
                    if cur_val > his_max_val:
                        tempdict[tempkey] =  (cur_val,i)
        for (k,v) in tempdict.items():
            reslist.append(v[1])
        choose_scheme = opti_data.iloc[reslist, :]
        # 保证dt
        c_gap_costs = choose_scheme.budget_c_gap.sum()
        b_gap_costs = choose_scheme.budget_b_gap.sum()

        #add rule here, in case dt spent too little or too much money
        dt_gap_costs = choose_scheme.dt_gap.sum()
        print("ROI最优化结果 : C端分配 : {} ,B端分配 : {}, 调度分配 : {} ".format(c_gap_costs,b_gap_costs,dt_gap_costs))
        if dt_gap_costs <  d_budget * 0.98 or dt_gap_costs > 1.2 * d_budget:
            if dt_gap_costs > 1.2 * d_budget:
                rescale = d_budget / dt_gap_costs * 0.98
                choose_scheme['dt_gap_tmp'] = choose_scheme.apply(lambda x: x.dt_gap * rescale, axis = 1)
                choose_scheme['cur_diaodu_subsidy'] = choose_scheme.apply(lambda x: x.cur_diaodu_subsidy - (x.dt_gap - x.dt_gap_tmp), axis = 1)
                choose_scheme['dt_gap'] = choose_scheme.apply(lambda x: x.dt_gap_tmp, axis = 1)
                del choose_scheme['dt_gap_tmp']
            else:
                #choose top diaodu cities
                money_gap = d_budget * 0.98 - dt_gap_costs
                #ddtmpall = choose_scheme[['city_id', 'pred_date','k2','total_gmv']].drop_duplicates().sort_values(['k2'], ascending = False)
                # use the following condition to avoid pingbi diaodu cities
                ddtmpall = opti_data.query("cur_diaodu_subsidy > 0")[['city_id', 'pred_date','k2','total_gmv']].drop_duplicates().sort_values(['k2'], ascending = False)

                ddtmp = ddtmpall.iloc[0 : int(len(ddtmpall)/3)]
                totalgmv = np.sum(ddtmp['total_gmv'])
                ddtmp['diaodu_add'] = ddtmp.apply(lambda x : x.total_gmv/totalgmv * money_gap * 0.67, axis = 1)
                choose_scheme_top = pd.merge(choose_scheme, ddtmp, on = ['city_id', 'pred_date','k2','total_gmv'], how = 'inner')
                choose_scheme_top['dt_gap'] = choose_scheme_top.apply(lambda x: x.dt_gap + x.diaodu_add if x.dt_gap >=0 else x.diaodu_add, axis = 1)
                choose_scheme_top['cur_diaodu_subsidy'] = choose_scheme_top.apply(lambda x: x.dt_gap + x.cur_diaodu_subsidy if x.cur_diaodu_subsidy >= x.subsidy_money_lowerbound else x.dt_gap + x.subsidy_money_lowerbound, axis = 1)

                if 'diaodu_add' in choose_scheme_top:
                    del choose_scheme_top['diaodu_add']

                ddtmp = ddtmpall.iloc[int(len(ddtmpall)/3) : ]
                totalgmv = np.sum(ddtmp['total_gmv'])
                ddtmp['diaodu_add'] = ddtmp.apply(lambda x : x.total_gmv/totalgmv * money_gap * 0.33, axis = 1)
                choose_scheme_tail = pd.merge(choose_scheme, ddtmp, on = ['city_id', 'pred_date','k2','total_gmv'], how = 'inner')

                choose_scheme_tail['dt_gap'] = choose_scheme_tail.apply(lambda x: x.dt_gap + x.diaodu_add if x.dt_gap >=0 else x.diaodu_add, axis = 1)
                choose_scheme_tail['cur_diaodu_subsidy'] = choose_scheme_tail.apply(lambda x: x.dt_gap + x.cur_diaodu_subsidy if x.cur_diaodu_subsidy >= x.subsidy_money_lowerbound else x.dt_gap + x.subsidy_money_lowerbound, axis = 1)
                if 'diaodu_add' in choose_scheme_tail:
                    del choose_scheme_tail['diaodu_add']
                choose_scheme = pd.concat([choose_scheme_top, choose_scheme_tail], ignore_index = True)

        dt_costs = choose_scheme.cur_diaodu_subsidy.sum()
        dt_gap_costs = choose_scheme.dt_gap.sum()
        cur_costs = c_gap_costs + b_gap_costs + dt_gap_costs
        gap = total_budget - cur_costs
        print("dt_costs is {}, dt_gap is {}, cur_costs is {}".format(dt_costs, dt_gap_costs, cur_costs))
        print('规划和预算的gap :',gap)
        if(gap < 0):
            cur_gap_costs = c_gap_costs + b_gap_costs
            choose_scheme['budget_c_gap'] = choose_scheme['budget_c_gap'].apply(lambda x: x + x * gap / cur_gap_costs)
            choose_scheme['budget_b_gap'] = choose_scheme['budget_b_gap'].apply(lambda x: x + x * gap / cur_gap_costs)
    else:
        print('The problem does not have an optimal solution.')
        choose_scheme = opti_data.query('(budget_c_gap ==0.0) and (budget_b_gap == 0.0) and (subsidy_money_lowerbound == cur_diaodu_subsidy)')

    return choose_scheme


def operation_rule(total_budget, optimal_result, params):
    # 运营：核销补贴率 > 千分之一
    print("总预算：",total_budget,"B端：",optimal_result['new_b_budget'].sum(),"C端：",optimal_result['new_c_budget'].sum(),"调度：",optimal_result['cur_diaodu_subsidy'].sum(), "加速卡:", optimal_result['ori_acc_budget'].sum())
    print(optimal_result.head())
    # 下边界处理
    optimal_result['new_c_budget'] = optimal_result.apply(lambda x: float(x.new_c_budget) if(x.new_c_budget/(x.total_gmv+1e-12) > 0.001) else 0.0, axis=1)
    optimal_result['new_b_budget'] = optimal_result.apply(lambda x: float(x.new_b_budget) if(x.new_b_budget/(x.total_gmv+1e-12) > 0.001) else 0.0, axis=1)
    print("下边界处理后:总预算:",total_budget,"B端:",optimal_result['new_b_budget'].sum(),"C端:",optimal_result['new_c_budget'].sum(),"调度:",optimal_result['cur_diaodu_subsidy'].sum(), "加速卡:", optimal_result['ori_acc_budget'].sum())

    print("optimal_result ", optimal_result.head())

    # 计算最终剩下的钱
    params['final_margin_budget'] = total_budget - optimal_result['new_b_budget'].sum() \
                                                 - optimal_result['new_c_budget'].sum() \
                                                 - optimal_result['cur_diaodu_subsidy'].sum() \
                                                 - optimal_result['ori_acc_budget'].sum()
    
    print("剩余的钱：",params['final_margin_budget'])
    print(optimal_result.isnull().any())
    add_money_flag = 1 if params['final_margin_budget'] > 0 else 0
    if add_money_flag:
        print('加钱! ')
        unalloc_rate = 0.04
    else:
        print('减钱! ')
        unalloc_rate = 1.0
    # 填坑钱 > 4%, 缩放不加钱。 
    """
    分别计算对应的补贴率活动， 如果补贴率在unalloc_rate内，计入已花费
    """
    c_cost = optimal_result.apply(lambda x: x.new_c_budget if(x.new_c_budget/(x.total_gmv+1e-12) < unalloc_rate) else 0.0, axis=1).astype(float).sum()
    b_cost = optimal_result.apply(lambda x: x.new_b_budget if(x.new_b_budget/(x.total_gmv+1e-12) < unalloc_rate) else 0.0, axis=1).astype(float).sum()
    td_cost = optimal_result.apply(lambda x: x.cur_diaodu_subsidy if(x.cur_diaodu_subsidy/(x.total_gmv+1e-12) < unalloc_rate) else 0.0, axis=1).astype(float).sum()
    diaodu_cost = optimal_result['cur_diaodu_subsidy'].astype(float).sum()
        
    params['already_spent_money'] = c_cost + b_cost
    print('already_spent_money:{} , C端:{} , B端:{}'.format(params['already_spent_money'], c_cost , b_cost))
    print('根据剩余金额（ {} ）缩放调整 ! '.format(params['final_margin_budget']))
    optimal_result['new_c_budget1'] = optimal_result.apply(lambda x: x.new_c_budget + x.new_c_budget * params['final_margin_budget']/params['already_spent_money']
                                                           if(x.new_c_budget/(x.total_gmv+1e-12) < unalloc_rate) else x.new_c_budget, axis=1)
    optimal_result['new_b_budget1'] = optimal_result.apply(lambda x: x.new_b_budget + x.new_b_budget * params['final_margin_budget']/params['already_spent_money']
                                                           if(x.new_b_budget/(x.total_gmv+1e-12) < unalloc_rate) else x.new_b_budget, axis=1)
#     optimal_result['new_b_budget1'] = optimal_result.apply(lambda x: x.new_b_budget + x.new_b_budget * params['final_margin_budget']/params['already_spent_money']
#                                                            if(x.new_b_budget/(x.total_gmv+1e-12) < unalloc_rate) else x.new_b_budget, axis=1)
        
    print("optimal_result",optimal_result.head())
    optimal_result['new_c_budget_rate'] = optimal_result['new_c_budget1'] / \
        (optimal_result['total_gmv'] + 1e-12)
    optimal_result['new_b_budget_rate'] = optimal_result['new_b_budget1'] / \
        (optimal_result['total_gmv'] + 1e-12)
    optimal_result['new_diaodu_budget_rate'] = optimal_result['cur_diaodu_subsidy'] / \
        (optimal_result['total_gmv'] + 1e-12)
        
    optimal_result['roi'] = (optimal_result['dgmv'] * optimal_result['total_gmv']) / (
        optimal_result['new_c_budget1'].astype(float) + optimal_result['new_b_budget1'].astype(float) + 1e-12 + optimal_result['cur_diaodu_subsidy'].astype(float) + 1e-12)

    print('最后分配结果: B端分配金额：{}, C端分配金额: {} , 加速卡分配金额:{} , 调度分配金额:{} '.format(
        optimal_result['new_b_budget1'].sum(), optimal_result['new_c_budget1'].sum(),optimal_result['ori_acc_budget'].sum() ,optimal_result['cur_diaodu_subsidy'].sum()))
    print(optimal_result.isnull().any())
    print('全部分配策略:')
    optimal_result['ori_c_budget'] = optimal_result['ori_c_budget'].astype(
        float)

    # 对平衡部分的钱按照活动gmv占比分发到填坑活动上
    optimal_result[['pangu_stg_detail','acc_card_stg_detail']] = optimal_result.apply(rescale_b_gap_money, axis=1,result_type = 'expand')
    
    optimal_result_ = optimal_result[['city_id', 'pred_date', 'ori_c_budget', 'new_c_budget1', 'new_c_budget_rate', 'ori_b_budget','ori_acc_budget','ori_acc_rate','pangu_stg_detail','acc_card_stg_detail',
                                      'new_b_budget1', 'new_b_budget_rate','cur_diaodu_subsidy','new_diaodu_budget_rate','subsidy_money_lowerbound'
                                      ,'subsidy_money_upperbound','lastweek_avg_diaodu_rate','lastweek_diaodu_gmv','type', 'k1', 'k2', 'k3','alpha',
                                      'intercept','succ_rate','total_gmv', 'dgmv', 'roi']].sort_values(by=['city_id', 'pred_date']).reset_index(drop=True)
    optimal_result_.columns = ['city_id', 'pred_date', 'ori_c_budget', 'new_c_budget', 'new_c_budget_rate', 'ori_b_budget','ori_acc_budget','ori_acc_rate','pangu_stg_detail','acc_card_stg_detail',
                                'new_b_budget', 'new_b_budget_rate','cur_diaodu_subsidy','new_diaodu_budget_rate','subsidy_money_lowerbound'
                                ,'subsidy_money_upperbound','lastweek_avg_diaodu_rate','lastweek_diaodu_gmv','type', 'k1', 'k2', 'k3','alpha',
                                'intercept','succ_rate','total_gmv', 'dgmv', 'roi']
    print("*********规划的最终结果：optimal_result_**********",optimal_result_.head(10))
    return optimal_result_

def callback_request(api, data):
    headers = {'Content-Type': 'application/json;charset=UTF-8'}
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    #resp = requests.post(api, data=data, headers=headers, timeout=None)
    print(data)
    resp = requests.post(api, data=data, headers=headers, timeout=None)
    print("*******返回结果测试，resp*******",resp.json()) 
    if resp.status_code != requests.codes.ok:
        print("****************返回结果测试 **********",resp.status_code)
        raise Exception('%s : %s' % (api, resp.text))
    return json.loads(resp.text)

def upload_to_gift(results, namespace, title, fmt='xlsx'):
    _, output = tempfile.mkstemp(prefix='smt_', suffix='.xlsx')
    results.to_excel(output, index=None)
    client = Minio('s3-gzpu.didistatic.com',
        access_key='AKDD00000000000VXSIXFN9EFRW5UM',
        secret_key='ASDDzpwZLeWuhaVaaSDdRzrJSVqLKlkxfnRWgPmY',
        secure=True,
    )
    filename = f'{namespace}/{datetime.now().strftime("%Y-%m-%d")}/{title}_{int(10000*random.random()):05d}.xlsx'
    resp = client.fput_object('budget-overwrite', filename, output)
    print(f'https://s3-gzpu.didistatic.com/budget-overwrite/{filename}')
    return f'https://s3-gzpu.didistatic.com/budget-overwrite/{filename}'

def send_message(title, env, msg, webhook=""):
	if webhook == "":
		webhook = "https://im-dichat.xiaojukeji.com/api/hooks/incoming/04b19429-d0a4-4475-986e-a1faa1bfa675"
	robot = DChatRobot(webhook)
	robot.send_message(
		f'【分配报告-{title}-{env}】',
		[{"text": msg},{"env": env}],
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

