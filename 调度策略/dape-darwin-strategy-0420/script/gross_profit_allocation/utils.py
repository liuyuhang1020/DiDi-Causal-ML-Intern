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
from minio import Minio
from ortools.sat.python import cp_model
from ortools.linear_solver import pywraplp
import matplotlib.pyplot as plt
import dataframe_image as dfi
import random
import datetime

logger = logging.getLogger(__name__)
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.set_option('expand_frame_repr', False)
MONEY_UNIT = 100
GAP_RATE_DEFAULT = 0.9
MIN_NUM_OF_DATA = 0
B_WEBHOOK="https://im-dichat.xiaojukeji.com/api/hooks/incoming/04b19429-d0a4-4475-986e-a1faa1bfa675"
C_WEBHOOK="https://im-dichat.xiaojukeji.com/api/hooks/incoming/8e56a5dd-dbaa-45b6-8339-47a30efe9b65"


def callback_request(api, data):
    headers = {'Content-Type': 'application/json;charset=UTF-8'}
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    #resp = requests.post(api, data=data, headers=headers, timeout=None)
    # print(data)
    resp = requests.post(api, data=data, headers=headers, timeout=None)
    # print("*******返回结果测试，resp*******",resp.json()) 
    if resp.status_code != requests.codes.ok:
        print("****************返回结果测试 **********",resp.status_code)
        raise Exception('%s : %s' % (api, resp.text))
    return json.loads(resp.text)

def get_gap_rate():
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
    return gap_rate

def get_b_rate_dict():
    # todo 1、通过接口调用，获取填坑预算比例和填坑加速卡补贴率率的离散值
    apollo_url = "http://10.88.128.149:32234/darwin/config/getapollo"
    # 盘古｜加速卡补贴率参数传递
    rate_params = {'namespace':'dape_darwin_cfg',
            'cfg_name':'b_rate_selection_list',
            'stg_name':'all'}
    b_gap_city_rate = callback_request(apollo_url,rate_params)
    accelerate_card_rate_dict = json.loads(b_gap_city_rate["data"]["accelerate_card"])
    pangu_rate_dict = json.loads(b_gap_city_rate["data"]["pangu"])
    
    return accelerate_card_rate_dict, pangu_rate_dict

def load_conf(conf_file):
    if not os.path.exists(conf_file):
        raise Exception('Failed to find config file: %s' % conf_file)
    with open(conf_file) as fconf:
        config = json.load(fconf)
        logger.info('config: %s' % config)
        return config


def fix_pkrate_func(x, th_pk_dd):
    thhf = round(float(x.treat_subsidy_hufan_rate_th), 3)
    pkhf = round(float(x.treat_subsidy_hufan_rate_pk), 3)
    if thhf in th_pk_dd: # thhf在补贴率字典中
        minpkhf = th_pk_dd[thhf]
        return max(pkhf, minpkhf)
    else: # thhf不在补贴率字典中
        thhf_keys = th_pk_dd.keys()
        # 找到字典中,和thhf最近的key
        closet_thhf = min(thhf_keys, key=lambda x: np.abs(x - thhf))
        minpkhf = th_pk_dd[closet_thhf]
        return max(pkhf, minpkhf)

def gen_top_cities(opti_data):
    city_list = list(opti_data.city_id.unique())
    # 分城市分天保留一条数据
    opti_data_uniq = opti_data.query("city_id in @city_list").drop_duplicates(subset=['city_id','pred_date'], keep='first')
    # 计算分城市的gmv均值
    opti_data_grp = opti_data_uniq.groupby('city_id').agg({'gmv_k':'mean', 'gmv_th':'mean'}).reset_index()
    opti_data_grp['gmv'] = opti_data_grp['gmv_k'] + opti_data_grp['gmv_th']
    #top 50的城市
    topcities = opti_data_grp.sort_values(by='gmv',ascending=False)['city_id'].values[:50]
    topcitystr = ','.join(map(str, topcities))
    return topcitystr


def decay_price_gap_for_lackcity(filter_opti_df, topcitystr):
    price_gaps = [0.02, 0.01, 0.005, 0.001]
    cur_city_list = []
    top_city_list = list(map(int, topcitystr.strip().split(',')))
    lack_city_list = top_city_list

    df_li = []
    for price_gap in price_gaps:
        print('price gap:',price_gap)
        tmpdf = filter_opti_df.query(
            "city_id in @lack_city_list and treat_subsidy_hufan_rate_th >= treat_subsidy_hufan_rate_pk + %.3f " % price_gap)
        df_li.append(tmpdf)
        cur_city_list.extend(tmpdf['city_id'].unique().tolist())
        lack_city_list = list(set(top_city_list) - set(cur_city_list))
        print("lack city:", lack_city_list)
        if len(lack_city_list) == 0:
            break
    df = pd.concat(df_li, ignore_index=True)
    return df


def linear_contraint(opti_data, total_budget, params, min_elem_weight = 0.000001):
    from datetime import datetime
    start_a = datetime.now()

    if total_budget <= 0.0:
        choose_scheme = opti_data.query(
            '(budget_c_gap ==0.0) and (budget_b_gap == 0.0) and (subsidy_money_lowerbound == cur_diaodu_subsidy)')
        return choose_scheme
    
    # 调度为b的25%, 同时去掉填坑已经占的比例
    holiday_weight = 0.3
    params['d_weight'] = 0.03
    #add diaodu weight
    d_budget = max(min(params['d_weight'] * params['ori_total_budget'] - params['dt_min_spent'],total_budget),1000) # dd rest budget

    print('total_budget:{}'.format(total_budget))
    print('b&c_budget:{}, d_budget:{}'.format( total_budget-d_budget, d_budget))
    
    opti_data['budget_c_gap'] = opti_data['budget_c_gap'].astype(float)
    opti_data['budget_b_gap'] = opti_data['budget_b_gap'].astype(float)
    opti_data['dt_gap'] = opti_data['dt_gap'].astype(float)

    x = {}
    solver = pywraplp.Solver.CreateSolver('CBC')
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
        send_message("调度T+N",params['is_online'], 'The problem does not have an optimal solution.')
        choose_scheme = opti_data.query('(budget_c_gap == 0.0) and (budget_b_gap == 0.0) and (subsidy_money_lowerbound == cur_diaodu_subsidy)')

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
    optimal_result['ori_c_budget'] = optimal_result['ori_c_budget'].astype(float)

    optimal_result_ = optimal_result[['city_id', 'pred_date', 'ori_c_budget', 'new_c_budget1', 'new_c_budget_rate', 'ori_b_budget','ori_acc_budget','ori_acc_rate',
                                      'new_b_budget1', 'new_b_budget_rate','cur_diaodu_subsidy','new_diaodu_budget_rate','subsidy_money_lowerbound'
                                      ,'subsidy_money_upperbound','lastweek_avg_diaodu_rate','lastweek_diaodu_gmv','type', 'k1', 'k2', 'k3','alpha',
                                      'intercept','succ_rate','total_gmv', 'dgmv', 'roi']].sort_values(by=['city_id', 'pred_date']).reset_index(drop=True)
    optimal_result_.columns = ['city_id', 'pred_date', 'ori_c_budget', 'new_c_budget', 'new_c_budget_rate', 'ori_b_budget','ori_acc_budget','ori_acc_rate',
                                'new_b_budget', 'new_b_budget_rate','cur_diaodu_subsidy','new_diaodu_budget_rate','subsidy_money_lowerbound'
                                ,'subsidy_money_upperbound','lastweek_avg_diaodu_rate','lastweek_diaodu_gmv','type', 'k1', 'k2', 'k3','alpha',
                                'intercept','succ_rate','total_gmv', 'dgmv', 'roi']
    print("*********规划的最终结果：optimal_result_**********",optimal_result_.head(10))
    return optimal_result_

def send_message(title, env, msg, webhook=""):
    if env == "test":
        print("this is test , no need notificate")
        return 
    if webhook == "":
        webhook = "https://im-dichat.xiaojukeji.com/api/hooks/incoming/839e4cbe-2c7a-4db0-b918-d1d0e0f8ff46"
    robot = DChatRobot(webhook)
    robot.send_message(
		f'【预算分配-{title}-{env}】',
		[{"text": msg},{"env": env}],
		False
	)

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


def report_message(title, env, pic_info, download_link, abn_event_link, gap_brate_event_link , webhook="" ):
	if webhook == "":
		webhook = "https://im-dichat.xiaojukeji.com/api/hooks/incoming/04b19429-d0a4-4475-986e-a1faa1bfa675"
		# webhook = "https://im-dichat.xiaojukeji.com/api/hooks/incoming/ac230690-40da-4123-85df-77ee6a07160b"
	robot = DChatRobot(webhook)
	if abn_event_link !="":
		abn_event_link = f'![这是图片]({abn_event_link})'
	if gap_brate_event_link != "":
		gap_brate_event_link = f'![这是图片]({gap_brate_event_link})'
	text_link = "" + abn_event_link + gap_brate_event_link
	robot.send_message(
		f'[**{pic_info}-{env}**]({download_link})',
		[{	
			"title": title,
			"text": text_link,
			"color": "#cdff19"
			}],
		True
	)
def upload_to_gift(output, namespace, title, fmt='xlsx'):

    client = Minio('s3-gzpu.didistatic.com',
        access_key='AKDD00000000000VXSIXFN9EFRW5UM',
        secret_key='ASDDzpwZLeWuhaVaaSDdRzrJSVqLKlkxfnRWgPmY',
        secure=True,
    )
    filename = f'{namespace}/{datetime.datetime.now().strftime("%Y-%m-%d")}/{title}_{int(10000*random.random()):05d}.{fmt}'
    resp = client.fput_object('budget-overwrite', filename, output)
    print(f'https://s3-gzpu.didistatic.com/budget-overwrite/{filename}')
    return f'https://s3-gzpu.didistatic.com/budget-overwrite/{filename}'

def allocation_info_check(gap_choose_scheme,daily_gmv_info):
    """
    1、实现功能：
        a,处理异常分配结果，上传到gift，获取链接 
        b,绘制gap和坑深的关系图，这里面需要用不同颜色标出深坑，上传到gift，获取链接
        c,将整个结果上传到gift，获取链接 
    2、主要处理逻辑：通过处理好数据后，对不同数据不同处理，upload gift 操作
    """
    # check  shape 
    allocation_df = pd.merge(gap_choose_scheme,daily_gmv_info, on = ['city_id','stat_date'],how = 'left')

    allocation_df = allocation_df[['city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr',
                            'cr_bar', 'b_rate','subsidy','tool', 'gmv', 'gmv_daily']]
    allocation_df['b_rate_daily'] = allocation_df['subsidy']/ allocation_df['gmv_daily']
    print(allocation_df.head())

    total_df=  allocation_df
    total_df['tool'] = total_df['tool'].apply(lambda x : "acc_card" if x == "加速卡" else "pangu")
    _, output = tempfile.mkstemp(prefix='smt_', suffix='.xlsx')
    total_df.to_excel(output, index=None)
    b_allocation_event_link = upload_to_gift(output,"预算分配报告","b_allocation_event_info",'xlsx')

    # 绘制gap和坑深之间的关系
    total_df["cr_gap_ori"] = total_df["cr_bar"] - total_df["cr"]
    total_df["duration"] =  allocation_df.apply(lambda x : (datetime.datetime.strptime(x.end_time,'%H:%M:%S')  - datetime.datetime.strptime(x.start_time,'%H:%M:%S')).total_seconds()/3600.0,axis=1 )
    pangu_df = total_df.query("tool == 'pangu' ")
    plt.scatter(pangu_df.cr_gap_ori,pangu_df.b_rate ,c = pangu_df.duration)
    plt.colorbar()
    plt.title('The relation between gap and subsidy',fontsize=15,color='blue')
    #定义坐标轴
    plt.xlabel('gap_rate',fontsize=15,color='r')
    plt.ylabel('subsidy_rate',fontsize=15,color='r')

    _, output_pangu_gap = tempfile.mkstemp(prefix='smt_', suffix='.jpg')
    plt.savefig(output_pangu_gap)
    plt.close()
    gap_brate_event_link = upload_to_gift(output_pangu_gap,"预算分配报告","gap_brate_relate_info",'jpg')

    # 获取异常补贴率活动的记录(all)
    abn_total_df = pangu_df.query("(cr_gap_ori >= 0.06 and b_rate <= 0.05) or (cr_gap_ori <= 0.03 and b_rate >= 0.1) ")
    abn_total_df = abn_total_df[['city_id','stat_date','fence_id','tool','start_time','end_time','cr_gap_ori','b_rate']]
    abn_total_df_head = abn_total_df.head(50)
    _, output_abn = tempfile.mkstemp(prefix='smt_', suffix='.jpg')
    dfi.export(abn_total_df_head, output_abn, fontsize=14, max_rows=None, max_cols=None, table_conversion='matplotlib', chrome_path=None)
    abn_event_link = upload_to_gift(output_abn,"预算分配报告","abnormal_event_info",'jpg')

    return b_allocation_event_link, gap_brate_event_link, abn_event_link, abn_total_df.shape[0]

def c_broadcast(all_df, df, is_online, pred_date, order_id):
    if df.shape[0] != MIN_NUM_OF_DATA:
        daily_info = df[['city_id','pred_date','t_gmv_pk','budget_pk','treat_subsidy_hufan_rate_pk','t_gmv_th','budget_th','treat_subsidy_hufan_rate_th']].rename(columns = {'treat_subsidy_hufan_rate_pk':'pk_hf_rate','treat_subsidy_hufan_rate_th':'th_hf_rate','t_gmv_pk':'pk_gmv','t_gmv_th':'th_gmv'})
        daily_info = daily_info.round({'pk_gmv':2, 'th_gmv':2, 'budget_pk':2, 'budget_th':2, 'pk_hf_rate':3, 'th_hf_rate':3})
        
        # 剩余天规划的总体信息保存成excel
        total_df=  all_df
        _, output = tempfile.mkstemp(prefix='smt_', suffix='.xlsx')
        total_df.to_excel(output, index=None)
        c_allocation_budget_link = upload_to_gift(output,"预算分配报告","c_allocation_budget_info",'xlsx')
        # T+1 预算的图片展示
        daily_info_head = daily_info.sort_values('pk_gmv',ascending = False).head(50).reset_index(drop=True)
        _, output_daily_info = tempfile.mkstemp(prefix='smt_', suffix='.jpg')
        dfi.export(daily_info_head, output_daily_info, fontsize=14, max_rows=None, max_cols=None, table_conversion='matplotlib', chrome_path=None)
        c_top50_budget_info_link = upload_to_gift(output_daily_info,"预算分配报告","c_top50_budget_info",'jpg')
        report_message( "TOP50城C端预算如下" , is_online, "["+pred_date+"]-"+str(order_id)+"-C端呼返预算信息", c_allocation_budget_link, c_top50_budget_info_link, "" ,webhook=C_WEBHOOK)
    else:
        print("No budget have been allocated")
    return 

def short_duration_check(start_time, end_time, b_rate):
    start_time = datetime.datetime.strptime(start_time,"%H:%M:%S")
    end_time = datetime.datetime.strptime(end_time,"%H:%M:%S")
    duration_hour = (end_time - start_time).seconds / 3600
    duration_check = 0
    if b_rate == 0.0 or b_rate >= 0.06:
        duration_check = 0
    elif duration_hour <= 3.0:  # 如果活动B补率在 0-6pp内，但是活动时长低于3小时
        duration_check = 1
    return duration_check

def allocation_info_daily_check(gap_b_table,daily_gmv_info):
    daily_gmv_info['pred_date'] = daily_gmv_info['stat_date']
    city_allocation_info = pd.merge(gap_b_table,daily_gmv_info, on = ['city_id','pred_date'],how = 'left')
    city_allocation_info = city_allocation_info[['city_id','pred_date','b_budget','acc_budget','gmv_daily']]
    city_allocation_info['b_rate'] = city_allocation_info['b_budget']/ city_allocation_info['gmv_daily']
    city_allocation_info['acc_rate'] = city_allocation_info['acc_budget']/ city_allocation_info['gmv_daily']
    print(city_allocation_info.head())
    
    # 城市补贴率信息明细 
    _, output = tempfile.mkstemp(prefix='smt_', suffix='.xlsx')
    city_allocation_info.to_excel(output, index=None)
    b_allocation_daily_link = upload_to_gift(output,"预算分配报告","b_allocation_daily_info",'xlsx')

    return b_allocation_daily_link
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

        