""""解析所有运行参数并做合并整理

上游交互协议文档: https://cooper.didichuxing.com/knowledge/2199520887308/2199860460347

A full resolved config example(using pprint):

{'artificial': [{'caller': 'b',
                 'city_id': 47,
                 'daily_b_rate': 0.03,
                 'date': '2022-12-08',
                 'fence_id': -1,
                 'time_range': ['06:30:00', '09:00:00']}],
 'artificial_df':          date  city_id            time_range  fence_id caller  daily_b_rate time_range_start time_range_end
0  2022-12-08       47  [06:30:00, 09:00:00]        -1      b          0.03         06:30:00       09:00:00,
 'budget_limit': 100000,
 'budget_limit_product': ['kuaiche', 'tehui'],
 'child_info': '',
 'cities': '1,2,3,4,5,47',
 'city_group': 'zhuanche_normal',
 'city_info_df':    city_id        date product_line caller
0        1  2022-12-02     zhuanche      b
1        2  2022-12-02     zhuanche      b
2        3  2022-12-02     zhuanche      b
3        4  2022-12-02     zhuanche      b
4        5  2022-12-02     zhuanche      b
5       47  2022-12-02     zhuanche      b,
 'city_limit_df':          date  city_id       pl caller                   limit  limit_max  limit_min
0  2022-12-02        1  kuaiche      b  {'max': 0.1, 'min': 0}        0.1          0,
 'end_date': '2022-12-07',
 'input_city_info': [{'caller': 'b',
                      'city_ids': [1, 2, 3, 4, 5, 47],
                      'date': '2022-12-02',
                      'product_line': 'zhuanche'}],
 'is_online': 'online',
 'local': {'callback_api': 'http://10.88.128.149:32234/darwin/',
           'callback_api_pre': 'http://10.88.151.15:20627/darwin/',
           'callback_api_test': 'http://dape-driver-test.didichuxing.com/darwin/',
           'daily_result_path': 'ly/bc_realloc_system/{}/{}/daily_{}.csv',
           'hdfs': {'ab_path': '/user/pricing_driver/norns_ab_ftp/',
                    'base_path': '/user/pricing_driver/competition_city_strategies/raw_grid_data',
                    'ftp_path': '/user/pricing_driver/norns_ftp/'},
           'holiday_set': ['2021-04-03',
                           '2021-04-04',
                           '2022-10-07'],
           'monitor': {'dchat_note_url': 'https://im-dichat.xiaojukeji.com/api/hooks/incoming/1d91b2f3-32ec-4720-a4ef-c3ecfb554242',
                       'monitor_type': 'stdout'},
           'online_request_bc_table_path': 'ly/bc_realloc_system/{}/online_request/bc_test.csv',
           'online_request_ori_b_table_path': 'ly/bc_realloc_system/{}/online_request/ori_b.csv',
           'online_request_ori_c_table_path': 'ly/bc_realloc_system/{}/online_request/ori_c.csv',
           'shumeng_bc_table_path': 'ly/bc_realloc_system/{}/shumeng/bc.csv',
           'shumeng_ori_b_table_path': 'ly/bc_realloc_system/{}/shumeng/ori_b.csv',
           'shumeng_ori_c_table_path': 'ly/bc_realloc_system/{}/shumeng/ori_c.csv',
           'tengnuo_city_lists': [1,
                                  255,
                                  63],
           'total_result_path': 'ly/bc_realloc_system/{}/{}/total_{}.csv'},
 'operator': 'wanhao',
 'order_id': 123,
 'rely_info': {'cr': '0.7',
               'cr_dt': '2022-12-02',
               'cr_type': 'objective_exp_openapi_pp',
               'gmv_dt': '2022-12-02',
               'proposed_budget': 0},
 'start_date': '2022-12-01',
 'step_end_date': '2022-12-02',
 'step_id': 3,
 'step_start_date': '2022-12-02',
 'step_type': 'B_T_1',
 'stg_constrain': {'budget_limit': {'key': 'budget_limit',
                                    'product_line': ['kuaiche', 'tehui'],
                                    'value': 100000},
                   'city_day_budget_rate_limit_list': [{'caller': 'b',
                                                        'city_id': 1,
                                                        'date': '2022-12-02',
                                                        'limit': {'max': 0.1,
                                                                  'min': 0},
                                                        'pl': 'kuaiche'}],
                   'total_gross_profit_fee': None},
 'stg_version': 'default',
 'target_list': [{'key': 'objective_exp_openapi_pp',
                  'product_line': ['kuaiche', 'tehui'],
                  'value': 0.7}],
 'trace_id': '133939344839dderf20',
 'trigger_time': '2022-11-29T10:28:46.620363+08:00',
 'version_list': ''}
"""


import os
import pandas as pd
# from loguru import logger
from utils import json_to_dict
import json
from callback import error_callback

CONF = {}


def load_mps_conf_old_protocol(args):
    # 接口参数
    conf = json_to_dict(args.parameter)
    # rely_info 在上一步被错误转义了，需要特殊处理
    conf.rely_info = json_to_dict(conf.rely_info.replace('\\', ''))
    conf.cities = get_mps_city_list_old_protocol(conf['ext_data']) # 获取城市列表
    print("cities:", conf.cities)
    conf.api_url = call_back_url(conf)
    conf.city_info_df = gen_mps_city_info(conf)
    conf.budget_limit_product = ''
    conf.city_limit_df = ''
    conf.city_group_info = gen_city_group(conf, version='old_proto') # 读取城市分框信息
    conf.city_group = 'mps'
    # 本地参数
    if not os.path.exists(args.config):
        print(f'Failed to find conf file: {args.config}')
        raise FileNotFoundError(args.config)
    with open(args.config) as conf_fd:
        conf_str = conf_fd.read().strip()
        conf.local = json_to_dict(conf_str)
    print('Arguments resolved. Full config:')
    global CONF
    CONF = conf
    return conf


def load_mps_conf_new_protocol(args):
    # 接口参数
    conf = json_to_dict(args.parameter)
    # rely_info 在上一步被错误转义了，需要特殊处理
    conf.rely_info = json_to_dict(conf.rely_info.replace('\\', ''))
    conf.cities = get_mps_city_list_new_protocol(conf) # 获取城市列表
    print("conf cis:", conf.cities)
    conf.api_url = call_back_url(conf)
    conf.city_info_df = gen_mps_city_info(conf)
    conf.budget_limit_product = ''
    conf.city_limit_df = ''
    conf.city_group_info = gen_city_group(conf, version='new_proto') # 读取城市分框信息
    conf.city_group = 'mps'
    # 本地参数
    if not os.path.exists(args.config):
        print(f'Failed to find conf file: {args.config}')
        raise FileNotFoundError(args.config)
    with open(args.config) as conf_fd:
        conf_str = conf_fd.read().strip()
        conf.local = json_to_dict(conf_str)
    print('Arguments resolved. Full config:')
    global CONF
    CONF = conf
    return conf


def load_conf(args):
    # 接口参数
    conf = json_to_dict(args.parameter)
    # rely_info 在上一步被错误转义了，需要特殊处理
    conf.rely_info = json_to_dict(conf.rely_info.replace('\\', ''))
    conf.city_info_df = city_info_to_df(conf.input_city_info)
    conf.cities = ','.join(str(c) for c in set(conf.city_info_df.city_id.tolist()))
    conf.artificial_df = artificial_to_df(conf.artificial)
    conf.budget_limit = float(conf.stg_constrain.budget_limit.value)
    conf.budget_limit_product = conf.stg_constrain.budget_limit.product_line
    conf.city_limit_df = city_rate_limit_to_df(conf.stg_constrain.city_day_budget_rate_limit_list)
    conf.api_url = call_back_url(conf)
    conf.city_group_info = ''
    # 本地参数
    if not os.path.exists(args.config):
        #logger.error(f'Failed to find conf file: {args.config}')
        print(f'Failed to find conf file: {args.config}')
        raise FileNotFoundError(args.config)
    with open(args.config) as conf_fd:
        conf_str = conf_fd.read().strip()
        conf.local = json_to_dict(conf_str)
    print('Arguments resolved. Full config:')
    global CONF
    CONF = conf
    return conf


def call_back_url(conf):
    if conf.is_online == "online":
        api_url = conf.callback_ip
    elif conf.is_online == "pre":
        api_url = conf.callback_ip
    elif conf.is_online == "test":
        api_url = conf.callback_ip
    return api_url


def city_info_to_df(city_info):
    date, product_line, caller, city_id = [], [], [], []
    for item in city_info:
        city_id.extend(item.city_ids)
        size = len(item.city_ids)
        date.extend([item.date] * size)
        product_line.extend([item.product_line] * size)
        caller.extend([item.caller] * size)
    df = pd.DataFrame({
        'city_id': city_id,
        'stat_date': date,
        'product_line': product_line,
        'caller': caller,
    })
    return df


def artificial_to_df(artificial):
    if artificial == None:
        return pd.DataFrame()
    # Convert to built-in dict
    data = [d.to_dict() for d in artificial]
    df = pd.DataFrame(data)
    df['time_range_start'] = df.time_range.map(lambda x: x[0])
    df['time_range_end'] = df.time_range.map(lambda x: x[1])
    return df


def city_rate_limit_to_df(rate_limit):
    if rate_limit != None:
        if len(rate_limit) != 0:
            data = [d.to_dict() for d in rate_limit]
            df = pd.DataFrame(data).rename(columns={'date': 'stat_date'})
            df['limit_max'] = df.limit.map(lambda x: x['max'])
            df['limit_min'] = df.limit.map(lambda x: x['min'])
            return df
    return pd.DataFrame(columns=['stat_date','city_id','pl','caller','limit','limit_min','limit_max'])


def gen_city_group(conf, version='old_proto'):
    '''读取城市分框信息'''
    total_bucket_info = []
    if version == 'new_proto':
        city_type_dict = conf['stg_constrain']['city_group_budget_amount_limit_list']
    elif version == 'old_proto':
        city_type_dict = conf['city_group_budget_amount_limit_list']

    if conf.stg_version == 'multi_target_v0' and len(city_type_dict) != 0:
        for idx, city_type_dict in enumerate(city_type_dict):
            dd = {}
            dd['start_date'] = city_type_dict['date_range'][0]
            dd['end_date'] = city_type_dict['date_range'][1]
            if dd['start_date'] < conf.step_start_date or dd['end_date'] > conf.end_date:
                error_callback("城市分框包含外层参数没有的日期！", conf)
            dd['city_list'] = city_type_dict['city_list']
            if len(dd['city_list']) > len(conf.cities):
                error_callback("城市分框中包含外层参数没有的城市！", conf)
            dd['budget_lowerbound'] = city_type_dict['limit']['min']
            dd['budget_upperbound'] = city_type_dict['limit']['max']
            total_bucket_info.append(dd)
    else:
        dd = {}
        dd['start_date'] = conf.step_start_date
        dd['end_date'] = conf.end_date
        dd['city_list'] = conf.cities
        dd['budget_lowerbound'] = 0
        if version == 'new_proto':
            dd['budget_upperbound'] = conf['stg_constrain']['budget_limit']['value']
        elif version == 'old_proto':
            dd['budget_upperbound'] = conf.budget_limit
        total_bucket_info.append(dd)
    return total_bucket_info


def get_mps_city_list_old_protocol(ext_data):
    """读取mps城市列表"""
    if len(json.loads(ext_data)['input_city_info']) == 0:
        print("输入城市为空 ")
    for line in json.loads(ext_data)['input_city_info']:
        if line['ca'] == 'dape-aps':
            city_list = line['cis']
            if len(city_list) > 0:
                citystr = ','.join(map(str, city_list))
                return citystr
            else:
                print("mps呼返的输入城市为空 ")
    return ''


def get_mps_city_list_new_protocol(conf):
    """读取mps城市列表"""
    errmsg = "外层输入的城市为空 "
    if len(conf['input_city_info']) == 0:
        error_callback(errmsg, conf)
    for line in conf['input_city_info']:
        if line['caller'] == 'dape-aps':
            city_list = line['city_ids']
            if len(city_list) > 0:
                citystr = ','.join(map(str, city_list))
                return citystr
            else:
                error_callback(errmsg, conf)
    error_callback(errmsg, conf)


def gen_mps_city_info(conf):
    pred_dates = list(pd.date_range(conf.step_start_date, conf.end_date).astype('str'))
    city_list = list(map(int, conf.cities.split(',')))
    if len(pred_dates) >= 1 and len(city_list) > 0:
        date_city_index = pd.MultiIndex.from_product([pred_dates, city_list], names=["stat_date", "city_id"])
        df_sample = pd.DataFrame(index=date_city_index).reset_index()
        return df_sample
    else:
        return pd.DataFrame(columns=["stat_date", "city_id"])
