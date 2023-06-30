from  utils import short_duration_check
from  const import MIN_NUM_OF_DATA
from  callback import error_callback
from ortools.linear_solver import pywraplp
import pandas as pd
import numpy as np
import json
# from loguru import logger


def gap_find_best_option(gap_pred_info, acc_budget, CONF):
    # 增加系数 CON 来保证异常率最小的前提下花费最小,一个任意足够大的数
    CON = 10000000
    x = {}
    solver = pywraplp.Solver.CreateSolver('CBC')
    solver.max_time_in_seconds = 240.0
    for i in range(len(gap_pred_info)):
        x[i] = solver.IntVar(0.0, 1, 'x[%i]' % (i))
    pangu_remain_budget = CONF.budget_limit - acc_budget
    print("********留给盘古填坑的预算：**********", pangu_remain_budget)

    solver.Add(
        solver.Sum([x[i] * gap_pred_info['subsidy'].iloc[i]
                    for i in range(0, len(x))]) <= CONF.budget_limit - acc_budget)

    solver.Add(solver.Sum([x[i] * gap_pred_info['subsidy'].iloc[i] for i in range(0, len(x))]) >= 0)

    for group_idx in list(set(gap_pred_info['group_idx'])):
        start_time = group_idx.split('_')[0]
        city = int(group_idx.split('_')[1])
        fence_id = int(group_idx.split('_')[2])
        stat_date = group_idx.split('_')[3]
        eventdf = gap_pred_info.query(
            "city_id == %d & fence_id == %d & stat_date == '%s' & start_time == '%s'  " %
            (city, fence_id, stat_date, start_time))
        idxlist = []
        if len(eventdf) >= 1:
            for j in range(0, len(eventdf)):
                idxlist.append(eventdf.iloc[j]['idx'])
            solver.Add(solver.Sum([x[idxlist[i]] for i in range(0, len(idxlist))]) == 1)

    print('Number of variables =', solver.NumVariables())
    print('Number of constraints =', solver.NumConstraints())
    # solver.Minimize(solver.Sum([x[i] * gap_pred_info['abn_rate'].iloc[i] for i in range(0, len(x))]))
    # 求解目标变为异常率最小的前提下补贴金额也尽可能小
    solver.Minimize(
        solver.Sum([
            x[i] * gap_pred_info['abn_rate'].iloc[i] * CON + x[i] * gap_pred_info['subsidy'].iloc[i]
            for i in range(0, len(x))
        ]))
    status = solver.Solve()

    reslist = []
    if status == pywraplp.Solver.OPTIMAL:
        print('Solution:')
        print('Objective value =', solver.Objective().Value())
        tempdict = {}
        for i in range(0, len(x)):
            cur_val = x[i].solution_value()
            if cur_val > 0.0:
                tempkey = '%s_%s' % (gap_pred_info['group_idx'].iloc[i],
                                     gap_pred_info['stat_date'].iloc[i])
                if not tempkey in tempdict:
                    tempdict[tempkey] = (cur_val, i)
                else:
                    his_max_val, his_idx = tempdict[tempkey]
                    if cur_val > his_max_val:
                        tempdict[tempkey] = (cur_val, i)
        for (k, v) in tempdict.items():
            reslist.append(v[1])
        gap_choose_scheme = gap_pred_info.iloc[reslist, :]
    else:
        error_callback("B端T+n分配：运筹求解失败",CONF)
    print("B端填坑阶段总花费：", gap_choose_scheme['subsidy'].sum())
    print("B端填坑总预算：", pangu_remain_budget)
    return gap_choose_scheme


def return_b_gap_info(choose_scheme, forecast_daily_data, asp_df, CONF):
    res_df = pd.DataFrame()
    for city in set(choose_scheme['city_id']):
        city_df = choose_scheme.query("city_id == %d" % (city))
        for day in set(city_df['stat_date']):
            city_day_df = city_df.query("stat_date == '%s'" % (day))
            for tool in set(city_day_df['caller']):
                city_day_tool_df = city_day_df.query("caller == '%s'" % (tool))
                temp = {}
                ori_budget = 0.0
                delta_rides = 0.0
                gmv = 0.0
                stg_detail = []
                for i in range(len(city_day_tool_df)):
                    ori_budget = ori_budget + city_day_tool_df['subsidy'].iloc[i]
                    delta_rides = delta_rides + city_day_df['delta_ride'].iloc[i]
                    stg_detail.append(city_day_tool_df['stg_detail'].iloc[i])
                if city in set(asp_df['city_id']):
                    asp = float(asp_df.query("city_id == '%d'" % (city))['asp'].values[0])
                elif forecast_daily_data.query("stat_date == '%s' & city_id == '%d'" %
                                               (day, city)).shape[0] != 0:
                    asp = float(
                        forecast_daily_data.query("stat_date == '%s' & city_id == '%d'" %
                                                  (day, city))['asp'].values[0])
                else:
                    error_callback('b端周分配 获取asp数据失败！',CONF)
                gmv = forecast_daily_data.query("stat_date == '%s' & city_id == '%d'" %
                                                (day, city))['total_gmv'].values[0]
                temp['city_id'] = city
                temp['pred_date'] = day
                temp['budget'] = round(ori_budget, 4)
                temp['stg_detail'] = stg_detail
                temp['delta_ride'] = round(delta_rides, 4)
                temp['gmv'] = round(gmv, 4)
                temp['asp'] = round(asp, 4)
                temp['delta_gmv'] = round(delta_rides * asp, 4)
                temp['caller'] = tool
                res_df = res_df.append(pd.DataFrame([temp]))
    return res_df


def filter_activate_subsidy_rate(choose_scheme, forecast_daily_data, city_day_budget_limit_df):
    ''' 根据输入对活动及天进行补贴率放缩'''
    # 对城市、天、抓手进行防缩
    if choose_scheme.shape[0] == MIN_NUM_OF_DATA:
        return choose_scheme
    city_day_caller_df = choose_scheme.groupby(['city_id', 'stat_date', 'caller']).agg({
        "subsidy":
        "sum"
    }).reset_index()
    city_day_caller_df = pd.merge(city_day_caller_df,
                                  forecast_daily_data,
                                  on=['city_id', 'stat_date'],
                                  how='left')
    city_day_caller_df[
        'subsidy_rate'] = city_day_caller_df['subsidy'] / city_day_caller_df['total_gmv']
    city_day_budget_limit_df = city_day_budget_limit_df.rename(columns={"date": "stat_date"})
    city_day_caller_df = pd.merge(city_day_budget_limit_df.query('caller != "all"'),
                                  city_day_caller_df,
                                  on=['city_id', 'stat_date', 'caller'],
                                  how='inner')
    city_day_caller_df[
        'reduce_rate'] = city_day_caller_df['limit_max'] / city_day_caller_df['subsidy_rate']
    if city_day_caller_df.query('reduce_rate < 1').shape[0] != MIN_NUM_OF_DATA:
        choose_scheme = pd.merge(choose_scheme,
                                 city_day_caller_df.query('reduce_rate < 1')[[
                                     'city_id', 'stat_date', 'caller', 'reduce_rate'
                                 ]],
                                 on=['city_id', 'stat_date', 'caller'],
                                 how='left').fillna(1)
        choose_scheme['b_rate'] = choose_scheme['b_rate'] * choose_scheme['reduce_rate']
        choose_scheme['subsidy'] = choose_scheme['subsidy'] * choose_scheme['reduce_rate']
        choose_scheme['delta_ride'] = choose_scheme['delta_ride'] * choose_scheme['reduce_rate']

    # 对城市、天进行防缩
    city_day_df = choose_scheme.groupby(['city_id', 'stat_date']).agg({
        "subsidy": "sum"
    }).reset_index()
    city_day_df = pd.merge(city_day_df,
                           forecast_daily_data,
                           on=['city_id', 'stat_date'],
                           how='left')
    city_day_df['subsidy_rate'] = city_day_df['subsidy'] / city_day_df['total_gmv']
    city_day_df = pd.merge(city_day_budget_limit_df.query('caller == "all"'),
                           city_day_df,
                           on=['city_id', 'stat_date'],
                           how='inner')
    city_day_df['reduce_rate'] = city_day_df['limit_max'] / city_day_df['subsidy_rate']
    if city_day_df.query('reduce_rate < 1').shape[0] != MIN_NUM_OF_DATA:
        choose_scheme = pd.merge(
            choose_scheme,
            city_day_df.query('reduce_rate < 1')[['city_id', 'stat_date', 'reduce_rate']],
            on=['city_id', 'stat_date'],
            how='left').fillna(1)
        choose_scheme['b_rate'] = choose_scheme['b_rate'] * choose_scheme['reduce_rate']
        choose_scheme['subsidy'] = choose_scheme['subsidy'] * choose_scheme['reduce_rate']
        choose_scheme['delta_ride'] = choose_scheme['delta_ride'] * choose_scheme['reduce_rate']
    return choose_scheme


def agg_event_2_city_options(x):
    '''聚合到城市维度，并将活动信息转换成dict存储在新的字段里'''

    event_dict = {
        "fence_id": x.fence_id,
        "start_time": x.start_time,
        "end_time": x.end_time,
        "amount": round(x.subsidy, 4),
        "interval_cr": round(x.cr, 4),
        "pit_depth": round(x.cr_bar - x.cr, 4),
        "interval_gmv": round(x.gmv, 4)
    }
    return event_dict


def get_hourly_indicator(x):
    '''
    返回对应补贴率下的该活动时段内的gmv，以及呼叫数
    '''
    ROI_FACTOR = 1.0  # 用来放缩在线时长对完单率的影响
    if int(x.stat_hour) < int(x.start_time[0:2]) or (int(x.stat_hour) >= int(x.end_time[0:2])
                                                     and x.end_time[3:5] != '30') or (int(
                                                         x.stat_hour) > int(x.end_time[0:2])):
        gmv_h = 0.0
        call_order_cnt_h = 0.0
        pred_cr = x.cr
    elif (int(x.stat_hour) == int(x.start_time[0:2])
          and x.start_time[3:5] == '30') or (int(x.stat_hour) == int(x.end_time[0:2])
                                             and x.end_time[3:5] == '30'):
        gmv_h = 0.5 * x.gmv_ratio * x.total_gmv  # 区县活动小时内的gmv，用来计算budget
        call_order_cnt_h = 0.5 * x.call_count_ratio * x.call_order_cnt  # 区县活动内小时的call，后续用来计算供需
        pred_cr = x.cr * (1 + x.delta_tsh * ROI_FACTOR)  # 如果补贴率为0，对应的pred_cr计算值应该是cr
    else:
        gmv_h = x.gmv_ratio * x.total_gmv
        call_order_cnt_h = x.call_count_ratio * x.call_order_cnt
        pred_cr = x.cr * (1 + x.delta_tsh * ROI_FACTOR)

    delta_cr_h = pred_cr - x.cr  # delta cr
    delta_rides_h = delta_cr_h * call_order_cnt_h
    cr_gap_h = x.cr_bar - pred_cr  # cr_gap aft sub
    # pred_cr_h = x.cr_bar if pred_cr > x.cr_bar else pred_cr   #FIXME 后续步长较大的情形
    pred_cr_h = pred_cr
    abn_rate_h = call_order_cnt_h * (pred_cr_h - x.cr_bar)**2  #成交异常率
    subsidy_h = gmv_h * x.b_rate

    return gmv_h, call_order_cnt_h, delta_cr_h, delta_rides_h, abn_rate_h, subsidy_h, cr_gap_h


def get_b_fillup_optimal_data(gap_dignosis, acc_elastic, pangu_elastic, forecast_daily_data,
                              asp_df,b_budget_limit_df,CONF):
    """
    获取B端填坑活动分配信息
    """
    # 1、加速卡填坑： 首先需要检查加速卡是否存在活动，如果不存在，直接进行盘古填坑，如果存在，需要先进行加速卡填坑
    acc_elastic_merge_df = pd.merge(gap_dignosis[gap_dignosis['caller'] == 'accelerate_card'],
                                    acc_elastic[acc_elastic['b_rate'] != 0],
                                    on=['city_id', 'stat_date', 'caller'],
                                    how='inner')
    if acc_elastic_merge_df.shape[0] != MIN_NUM_OF_DATA:
        acc_elastic_merge_df = acc_elastic_merge_df[
            acc_elastic_merge_df['stat_hour'] >= acc_elastic_merge_df['start_hour']][
                acc_elastic_merge_df['stat_hour'] <= acc_elastic_merge_df['end_hour']]
        acc_elastic_merge_df['acc_index'] = ['_'.join(i) for i in acc_elastic_merge_df[['start_time', 'city_id', 'fence_id', 'stat_date']].values.astype(str) ]  
        # 约束加速卡补贴满足上限，并返回加速卡 城市、天 花费
        acc_elastic_merge_df,valid_acc_budeget_df = sta_and_con_acc_budget(acc_elastic_merge_df,b_budget_limit_df)

        acc_elastic_merge_df[[
            'gmv_h', 'call_order_cnt_h', 'delta_cr_h', 'delta_ride_h', 'abn_rate_h', 'subsidy_h',
            'cr_gap_h'
        ]] = acc_elastic_merge_df.apply(get_hourly_indicator, axis=1, result_type='expand')
        #  中间产物，通过该表获取加速卡活动后分小时的cr变化
        acc_hourly_delta_cr = acc_elastic_merge_df[[
            'city_id', 'stat_date', 'fence_id', 'caller', 'stat_hour', 'cr', 'cr_bar', 'delta_cr_h'
        ]]
        # 聚合到活动粒度,和盘古规划活动一起进行join，作为最后输出
        acc_event_info = acc_elastic_merge_df.groupby([
            'city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr', 'cr_bar', 'b_rate'
        ],
                                                      as_index=False).agg({
                                                          'gmv_h': 'sum',
                                                          'abn_rate_h': 'sum',
                                                          'subsidy_h': 'sum',
                                                          'cr_gap_h': 'mean',
                                                          'delta_ride_h': 'sum'
                                                      }).rename(
                                                          columns={
                                                              'gmv_h': 'gmv',
                                                              'abn_rate_h': 'abn_rate',
                                                              'subsidy_h': 'subsidy',
                                                              'cr_gap_h': 'cr_gap',
                                                              'delta_ride_h': 'delta_ride'
                                                          })
        acc_event_info['caller'] = 'accelerate_card'
        print("加速卡活动数: {} ,加速卡填坑花费:{} ".format(acc_event_info.shape,
                                                     acc_event_info['subsidy'].sum()))
    else:
        acc_hourly_delta_cr = pd.DataFrame(columns=[
            'city_id', 'stat_date', 'fence_id', 'caller', 'stat_hour', 'cr', 'cr_bar', 'delta_cr_h'
        ])
        acc_event_info = pd.DataFrame(columns=[
            'city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr', 'cr_bar', 'b_rate',
            'gmv', 'abn_rate', 'delta_ride', 'subsidy', 'caller'
        ])
        valid_acc_budeget_df = pd.DataFrame(columns=['city_id','stat_date','acc_budget'])
        print("加速卡活动数: 0 ,加速卡填坑花费: 0.0 ")
    # 2、盘古填坑
    # 首先加工盘古活动的分小时弹性数据
    print(gap_dignosis.head())
    print(pangu_elastic.head())
    gap_dignosis = gap_dignosis.astype({'city_id': 'int', 'stat_date': 'str'})
    pangu_elastic = pangu_elastic.astype({'city_id': 'int', 'stat_date': 'str'})
    pangu_elastic_merge_df = pd.merge(gap_dignosis[gap_dignosis['caller'] == 'b'],
                                      pangu_elastic,
                                      on=['city_id', 'stat_date', 'caller'],
                                      how='inner')
    pangu_elastic_merge_df = pangu_elastic_merge_df[
        pangu_elastic_merge_df['stat_hour'] >= pangu_elastic_merge_df['start_hour']][
            pangu_elastic_merge_df['stat_hour'] <= pangu_elastic_merge_df['end_hour']]

    pangu_elastic_merge_df = pangu_elastic_merge_df.drop(pangu_elastic_merge_df[
        pangu_elastic_merge_df['end_hour'] -
        pangu_elastic_merge_df['start_hour'] >= 8][pangu_elastic_merge_df['b_rate'] >= 0.08].index)
    if pangu_elastic_merge_df.shape[0] == MIN_NUM_OF_DATA:
        gap_choose_scheme = pd.DataFrame(columns=[
                'city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr', 'cr_bar', 'b_rate',
                'gmv', 'abn_rate', 'delta_ride', 'subsidy', 'caller'
            ])
    else:
        print(CONF.city_group)
        if CONF.city_group == "mps" or CONF.city_group == "chuzuche_normal":
            pangu_elastic_merge_df["duration_check"] = pangu_elastic_merge_df.apply(
                lambda x: short_duration_check(x.start_time, x.end_time, x.b_rate),
                axis=1,
                result_type='expand')
        elif CONF.city_group == "zhuanche_normal":
            pangu_elastic_merge_df["duration_check"] = 0
        else:
            error_callback("B端T+n分配：不支持输入城市类型",CONF)
        print(pangu_elastic_merge_df.query("duration_check == 1 "))
        pangu_elastic_merge_df = pangu_elastic_merge_df.drop(
            pangu_elastic_merge_df[pangu_elastic_merge_df['duration_check'] == 1].index)

        pangu_merge_acc_cr = pd.merge(pangu_elastic_merge_df,
                                    acc_hourly_delta_cr,
                                    on=['city_id', 'stat_date', 'stat_hour'],
                                    how='left',
                                    suffixes=('', '_acc'))
        pangu_merge_acc_cr['delta_cr_h'] = pangu_merge_acc_cr['delta_cr_h'].fillna(0)
        pangu_merge_acc_cr['cr_ori'] = pangu_merge_acc_cr['cr']
        pangu_merge_acc_cr['cr'] = pangu_merge_acc_cr['cr_ori'] + pangu_merge_acc_cr[
            'delta_cr_h']  # cr update

        print("pangu_merge_acc_cr columns ", pangu_merge_acc_cr.columns)

        df = pangu_merge_acc_cr.drop(
            labels=['fence_id_acc', 'fence_id_acc', 'caller_acc', 'cr_acc', 'cr_bar_acc', 'delta_cr_h'],
            axis=1)
        if df.shape[0] != 0:
            # 指标加工
            df[[
                'gmv_h', 'call_order_cnt_h', 'delta_cr_h', 'delta_ride_h', 'abn_rate_h', 'subsidy_h',
                'cr_gap_h'
            ]] = df.apply(get_hourly_indicator, axis=1, result_type='expand')
            # 聚合到活动粒度,然后进行规划
            event_info = df.groupby([
                'city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr', 'cr_bar', 'b_rate'
            ],
                                    as_index=False).agg({
                                        'gmv_h': 'sum',
                                        'abn_rate_h': 'sum',
                                        'subsidy_h': 'sum',
                                        'cr_gap_h': 'mean',
                                        'delta_ride_h': 'sum'
                                    }).rename(
                                        columns={
                                            'gmv_h': 'gmv',
                                            'abn_rate_h': 'abn_rate',
                                            'subsidy_h': 'subsidy',
                                            'cr_gap_h': 'cr_gap',
                                            'delta_ride_h': 'delta_ride'
                                        })

            event_info['caller'] = 'b'
            print("盘古活动数: {} ,盘古填坑花费:{} ".format(event_info.shape, event_info['subsidy'].sum()))
            print("盘古填坑分布:", event_info.groupby("stat_date").count())

            # 3、填坑规划求解
            gap_pred_info = event_info.reset_index(drop=True)
            gap_pred_info['idx'] = list(range(gap_pred_info.shape[0]))
            gap_pred_info['group_idx'] = [
                '_'.join(i) for i in gap_pred_info[['start_time', 'city_id', 'fence_id', 'stat_date'
                                                    ]].values.astype(str)
            ]  # 添加唯一idx ，作为该区县时段唯一标识
            # 过滤盘古候选集，满足城市、天补贴率约束
            gap_pred_info = sta_and_con_pangu_budget(gap_pred_info,b_budget_limit_df,valid_acc_budeget_df)
            print("盘古填坑规划准备数据 info:", gap_pred_info.shape, gap_pred_info.head())
            gap_choose_scheme = gap_find_best_option(gap_pred_info, acc_event_info['subsidy'].sum(), CONF)
        else:
            gap_choose_scheme = pd.DataFrame(columns=[
                'city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr', 'cr_bar', 'b_rate',
                'gmv', 'abn_rate', 'delta_ride', 'subsidy', 'caller'
            ])
    # 4、加速卡和盘古结果融合
    gap_choose_scheme = gap_choose_scheme[gap_choose_scheme['subsidy'] != 0]  # 取实际花钱的活动
    if acc_event_info.shape[0] != MIN_NUM_OF_DATA:
        gap_choose_scheme = pd.concat([gap_choose_scheme[acc_event_info.columns], acc_event_info])
    else:
        gap_choose_scheme = gap_choose_scheme[[
            'city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr', 'cr_bar', 'b_rate',
            'gmv', 'abn_rate', 'subsidy', 'caller', 'delta_ride'
        ]]
    print("加速卡和盘古结果活动总数：", gap_choose_scheme.shape)

    # # 5、对补贴率进行约束
    # gap_choose_scheme = filter_activate_subsidy_rate(gap_choose_scheme, forecast_daily_data,
    #                                                  CONF.city_limit_df)
    # print(gap_choose_scheme.columns)
    # print(gap_choose_scheme.head)

    # 6、聚合
    if gap_choose_scheme.shape[0] != 0:
        gap_choose_scheme["stg_detail"] = gap_choose_scheme.apply(agg_event_2_city_options, axis=1)
        # 聚合到抓手、天
        gap_b_table = return_b_gap_info(gap_choose_scheme, forecast_daily_data, asp_df,CONF)
        rides_increase = gap_b_table['delta_ride'].sum()
        gap_b_table = gap_b_table[[
            'city_id', 'pred_date', 'budget', 'stg_detail', 'delta_ride', 'gmv', 'asp', 'delta_gmv',
            'caller'
        ]]
    else:
        gap_b_table = pd.DataFrame(columns=[
            'city_id', 'pred_date', 'budget', 'stg_detail', 'delta_ride', 'gmv', 'asp', 'delta_gmv',
            'caller'
        ])
    print("填坑结果-gap_b_table:", gap_b_table.head())
    return gap_b_table


def extract_json_format_b_budget(df, strategy_type_str, forecast_daily_data, CONF):
    ''' 将策略转化为json'''
    b_budget_dict = {}
    rely_info = {}
    rely_info['gmv_dt'] = CONF.rely_info.gmv_dt
    cr = int(strategy_type_str[-2:]) / 100
    rely_info['cr'] = str(cr)
    rely_info['cr_dt'] = CONF.rely_info.cr_dt
    rely_info['cr_type'] = CONF.rely_info.cr_type
    for version in json.loads(CONF.version_list):
        if version['cr'] == cr:
            v = version['version_code']
            break
    b_budget_dict['rely_info'] = rely_info
    b_budget_dict['version'] = v
    df_sample = pd.merge(CONF.city_info_df,
                         forecast_daily_data,
                         on=['city_id', 'stat_date'],
                         how='left').rename(columns={
                             'stat_date': 'pred_date',
                             'total_gmv': 'gmv'
                         })
    df_sample = df_sample[['city_id', 'pred_date', 'caller', 'gmv', 'product_line']]
    df_sample_total = pd.merge(
        df_sample,
        df[['city_id', 'pred_date', 'budget', 'stg_detail', 'delta_ride', 'delta_gmv', 'caller']],
        on=['city_id', 'pred_date', 'caller'],
        how='left')
    df_sample_total['budget'] = df_sample_total['budget'].fillna(0)
    df_sample_total['stg_detail'] = df_sample_total['stg_detail'].fillna('')
    df_sample_total['delta_ride'] = df_sample_total['delta_ride'].fillna(0)
    df_sample_total['delta_gmv'] = df_sample_total['delta_gmv'].fillna(0)

    data = []
    for index, city_day_df in df_sample_total.iterrows():
        if city_day_df['stg_detail'] == '':
            temp = []
        else:
            temp = city_day_df['stg_detail']
        city_day_data_dict = {}
        city_day_data_dict['city'] = city_day_df['city_id']
        city_day_data_dict["product_line"] = city_day_df['product_line']
        city_day_data_dict["caller"] = city_day_df['caller']
        city_day_data_dict['stat_date'] = city_day_df['pred_date']
        city_day_data_dict['stg_detail'] = temp
        city_day_data_dict['amount'] = city_day_df['budget']
        city_day_data_dict['gmv'] = city_day_df['gmv']
        city_day_data_dict['step_type'] = CONF.step_type
        data.append(city_day_data_dict)

    print('df_sample len:', len(df_sample_total))
    print('return data:', len(data))
    b_budget_dict['data'] = data
    ext_data = {}
    # 计算周成交率cr(总体cr和分城市的cr)

    df['delta_ride'] = df['delta_ride'].astype('float')
    df_day = df.groupby('city_id').agg({'delta_ride': 'sum'}).reset_index()
    base_call_finsh = forecast_daily_data.groupby('city_id').agg({
        'total_finish_order_cnt': 'sum',
        'call_order_cnt': 'sum'
    }).reset_index()
    call_finish_rides = pd.merge(base_call_finsh, df_day, on=['city_id'], how='left').fillna(0)
    call_finish_rides = call_finish_rides.astype({
        'delta_ride': 'float',
        'total_finish_order_cnt': 'float',
        'call_order_cnt': 'float',
        'city_id': 'int'
    })
    call_finish_rides['cr_city'] = (
        call_finish_rides['delta_ride'] +
        call_finish_rides['total_finish_order_cnt']) / call_finish_rides['call_order_cnt']
    print('call_finish_rides')
    print(call_finish_rides)
    city_cr_aft_sub = dict(zip(call_finish_rides['city_id'], round(call_finish_rides['cr_city'],
                                                                   4)))
    cr_aft_sub = (call_finish_rides['delta_ride'].sum() +
                  call_finish_rides['total_finish_order_cnt'].sum()
                  ) / call_finish_rides['call_order_cnt'].sum()
    city_cr_aft_sub['0'] = round(cr_aft_sub, 4)
    ext_data['sum_cr_rate'] = city_cr_aft_sub

    delta_gmv_all = df['delta_gmv'].sum()
    ext_data['delta_gmv'] = round(delta_gmv_all, 4)
    b_budget_dict['ext_data'] = ext_data
    return b_budget_dict


def generate_b_fillup_json(b_budget_list,CONF):
    b_fillup_ans = {
        "order_id": CONF.order_id,
        "step_id": CONF.step_id,
        "trace_id": CONF.trace_id,
        "version_info": json.dumps(b_budget_list)
    }
    return b_fillup_ans

def get_daily_budget_limit(forecast_daily_data,CONF):
    # ['stat_date','city_id','pl','caller','limit','limit_min','limit_max']
    b_budget_limit_df = pd.merge(CONF.city_limit_df,forecast_daily_data[['city_id','stat_date','total_gmv']],on = ['city_id','stat_date'],how = 'left')
    b_budget_limit_df['limit_min_amount'] = b_budget_limit_df['limit_min'] * b_budget_limit_df['total_gmv']
    b_budget_limit_df['limit_max_amount'] = b_budget_limit_df['limit_max'] * b_budget_limit_df['total_gmv']
    print('b_budget_limit_df')
    print(b_budget_limit_df)
    return b_budget_limit_df

def sta_and_con_acc_budget(acc_elastic_merge_df,b_budget_limit_df):
    '''统计加速卡花费和总预算的gap，并过滤满足条件的加速卡情况'''
    df = acc_elastic_merge_df.copy()
    init_columns = acc_elastic_merge_df.columns
    
    acc_elastic_merge_df[['gmv_h', 'call_order_cnt_h', 'delta_cr_h', 'delta_ride_h', 'abn_rate_h', 'subsidy_h','cr_gap_h']] = acc_elastic_merge_df.apply(get_hourly_indicator, axis=1, result_type='expand')
    acc_budeget_df = acc_elastic_merge_df.groupby(['acc_index','start_time', 'city_id', 'fence_id', 'stat_date','cr']).agg({"subsidy_h":"sum"}).reset_index()
    acc_budeget_df = acc_budeget_df.sort_values(['city_id','stat_date','cr','acc_index'],ascending = True)
    
    city_id_init = acc_budeget_df.loc[0, "city_id"]
    stat_date_init = acc_budeget_df.loc[0, "stat_date"]
    
    for index, cor in acc_budeget_df.iterrows():
        if cor['city_id'] == city_id_init and cor['stat_date'] == stat_date_init:
            cal += cor['subsidy_h']
        else:
            cal = 0
        city_id_init = cor['city_id']
        stat_date_init = cor['stat_date']
        acc_budeget_df.loc[index, 'cal_budget'] = cal
    
    acc_budeget_df = pd.merge(acc_budeget_df,b_budget_limit_df,on = ['city_id','stat_date'],how = 'right').query('cal_budget > limit_max_amount')
    invalid_acc_index_list = list(acc_budeget_df['acc_index'].unique())
    print('invalid_acc_index_list',invalid_acc_index_list)
    valid_acc_budeget_df = acc_budeget_df.query('acc_index not in @invalid_acc_index_list').groupby(['city_id','stat_date']).agg({"subsidy_h":"sum"}).reset_index().rename(columns={'subsidy_h':'acc_budget'})

    return df.query('acc_index not in @invalid_acc_index_list')[init_columns], valid_acc_budeget_df
    
def filter_invalid_candicate(df):
    # 过滤填坑后异常率高于不填的候选集
    df['pre_cr_gap'] = round(abs(df['cr_bar'] - df['cr']),4)
    df['aft_cr_gap'] = round(abs(df['cr_gap']),4)
    print('盘古填坑过多的补贴率：')
    print(df.query('pre_cr_gap < aft_cr_gap'))
    return df.query('pre_cr_gap >= aft_cr_gap')
    
def cal_cur_cost(df, pangu_budget_df):
    df = df.sort_values(['group_idx', 'b_rate'])
    cal_budget = 0
    group_idx_init = df.loc[0, "group_idx"]
    for index, cor in df.iterrows():
        if cor['group_idx'] == group_idx_init:
            cur_budget = cor['subsidy'] - cal_budget
            cal_budget = cor['subsidy']
        else:
            cur_budget = cor['subsidy']
            cal_budget = 0
        group_idx_init = cor["group_idx"]
        df.loc[index, 'cur_budget'] = cur_budget
    df = df.sort_values(['city_id','stat_date','cr','group_idx', 'b_rate'])
    cal = 0
    city_id_init = df.loc[0, "city_id"]
    stat_date_init = df.loc[0, "stat_date"]
    for index, cor in df.iterrows():
        if cor['city_id'] == city_id_init and cor['stat_date'] == stat_date_init:
            cal += cor['cur_budget']
        else:
            cal = 0
        city_id_init = cor['city_id']
        stat_date_init = cor['stat_date']
        df.loc[index, 'cal_budget'] = cal
    df = pd.merge(df,pangu_budget_df,on = ['city_id','stat_date'],how = 'left')
    df['pangu_budget'] =  df['pangu_budget'].fillna(np.inf)
    valid_df = df.query('cal_budget <= pangu_budget')
    print('valid_df,',valid_df.shape)
    zero_df = df.query('cal_budget > pangu_budget and b_rate == 0')
    print('zero_df,',zero_df.shape)
    return pd.concat([valid_df,zero_df])

def sta_and_con_pangu_budget(df,b_budget_limit_df,valid_acc_budeget_df):
    pangu_budget_df = pd.merge(b_budget_limit_df,valid_acc_budeget_df,on = ['city_id','stat_date'],how = 'left')
    pangu_budget_df['acc_budget'] = pangu_budget_df['acc_budget'].fillna(0)
    pangu_budget_df['pangu_budget'] = pangu_budget_df['limit_max_amount'] - pangu_budget_df['acc_budget']
    print('pangu_budget_df')
    print(pangu_budget_df)
    df = filter_invalid_candicate(df)
    df = cal_cur_cost(df,pangu_budget_df[['city_id','stat_date','pangu_budget']])
    df = df.drop(['group_idx','idx'],1)
    df['idx'] = list(range(df.shape[0]))
    df['group_idx'] = ['_'.join(i) for i in df[['start_time', 'city_id', 'fence_id', 'stat_date']].values.astype(str)]  
    return df

def b_fillup_and_gererate_json(fixed_threshold_list, gap_dignosis_all_strategy, acc_elastic,
                               pangu_elastic, forecast_daily_data, asp_df,CONF):
    b_budget_limit_df = get_daily_budget_limit(forecast_daily_data,CONF)
    b_budget_list = []
    for i in range(len(fixed_threshold_list)):
        gap_dignosis = gap_dignosis_all_strategy[gap_dignosis_all_strategy['strategy_type'] ==
                                                 fixed_threshold_list[i]].drop('strategy_type', 1)
        gap_b_table_df = get_b_fillup_optimal_data(gap_dignosis, acc_elastic, pangu_elastic,
                                                   forecast_daily_data, asp_df,b_budget_limit_df,CONF)
        # 结果处理
        b_budget_dict = extract_json_format_b_budget(gap_b_table_df, fixed_threshold_list[i],
                                                     forecast_daily_data,CONF)
        b_budget_list.append(b_budget_dict)
       
    return b_budget_list
