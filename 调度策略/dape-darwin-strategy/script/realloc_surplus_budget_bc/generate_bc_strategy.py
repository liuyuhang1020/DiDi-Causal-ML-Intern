#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
"""
import sys
import json
import argparse
import logging
import requests
from datetime import datetime, timedelta
from bc_strategy import Strategy
from utils import load_conf, execute_cmd, execute_cmd_and_get_result, time_to_float_hour, hdfs_path_exist, get_next_week_time,get_after_14_day,callback_request

logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_parameter(config, mode):
    param_str = config.parameter
    parsed = {}
    print("%s" % param_str)
    # with open('%s'%param_str) as f:
    #    param = json.load(f)
    #    f.close()
    param = json.loads(param_str)
    # '''
    parsed['is_online'] = param['is_online']
    parsed['trace_id'] = param['trace_id']
    parsed['order_id'] = param['order_id']
    parsed['remove_date'] = param['remove_date']
    parsed['city_list'] = param['city_list']
    parsed['budget'] = param['budget']  # fen
    parsed['operator'] = param['operator']
    parsed['zq_city'] = param['zq_city_list']
    if 'blackdict' in param:
        parsed['blackdict'] = param['blackdict']
    else:
        parsed['blackdict'] = []
    if(mode != 0):
        parsed['trigger_time'] = param['trigger_time']
        parsed['start_date'] = param['start_date']
        parsed['end_date'] = param['end_date']
    else:
        # 数梦任务，保留更多的数据
        # next_week_st, next_week_end = get_next_week_time(args.date)
        start_t, end_t = get_after_14_day(args.date)
        parsed['trigger_time'] = args.date
        parsed['start_date'] = start_t
        parsed['end_date'] = end_t
    # # '''
    # if 'request' in param:
    #     stg_param = json.loads(param['request'])
    #     logger.info('parameters %s ' % str(param))
    #     key = None
    #     try:
    #         parsed['budget'] = int(stg_param['budget'])
    #         parsed['trace_id'] = stg_param['trace_id']
    #         parsed['order_id'] = int(stg_param['order_id'])
    #         parsed['is_online'] = stg_param['is_online']
    #         parsed['start_date'] = stg_param['start_date']
    #         parsed['end_date'] = stg_param['end_date']
    #         parsed['remove_date'] = stg_param['remove_date']
    #         parsed['city_list'] = stg_param['city_list']
    #         parsed['operator'] = stg_param['operator']
    #         parsed['trigger_time'] = stg_param['trigger_time']
    #     except:
    #         raise Exception('Failed to parse %s from input parameter' % key)
    return parsed


def stg_callback(api, data):
    headers = {'Content-Type': 'application/json;charset=UTF-8'}
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    #resp = requests.post(api, data=data, headers=headers, timeout=None)
    print(data)
    resp = requests.post(api, data=data, timeout=None)
    if resp.status_code != requests.codes.ok:
        raise Exception('%s : %s' % (api, resp.text))
    return json.loads(resp.text)


def main(args):
    # 解析json文件
    config = load_conf(args.config)
    if args.parameter == '':
        return
    mode = int(args.mode)
    # 解析输入参数
    stg_param = parse_parameter(args, mode)
    logger.info('parsed parameters: %s' % str(stg_param))
    stg_param['cur_day'] = args.date
    stg = Strategy(config, stg_param, mode)
    # todo B端填坑，优化成交异常率
    stg.generate_b_gap_result()
    # 0 shumeng request
    # 1 online request, shumeng finish
    # 2 online request, shumeng not finish
    if(mode == 0):
        # 数据生成模块最后分配结果
        stg.preprocessed_data_save()
        return
    elif(mode == 1):
        # 线性规划模块
        stg_results = stg.generate_ouput_data()
    elif(mode == 2):
        # 数据生成模块 和 线性规划模块
        stg.preprocessed_data_save()
        stg_results = stg.generate_ouput_data()
        # stg.preprocessed_data_remove()
    else:
        print('mode error!')
        return
    # 数据回传接口
    stg_api_url = stg._api_url + "decision/adddecisiondetail"
    print("finish to generate strategy, result: ", stg_api_url)
    # print(stg_results)
    resp = callback_request(stg_api_url, stg_results)
    print('callback response: %s' % resp)
    if int(resp.get('errno', -1)) != 0:
        print('Failed to callback with strategy result!!!')
        raise Exception('Failed to callback with strategy result!!!')


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--config', help='配置文件', default='')
    arg_parser.add_argument('--date', help='活动计算日期，格式:yyyy-mm-dd format',
                            default=(datetime.now() + timedelta(days=0)).strftime('%Y-%m-%d'))
    arg_parser.add_argument('--parameter', help='活动参数', default='')
    arg_parser.add_argument('--res_cnt', help='生成结果数量', default=3)
    arg_parser.add_argument('--mode', help='xman or shumeng', default=-1)
    args = arg_parser.parse_args()

    main(args)
