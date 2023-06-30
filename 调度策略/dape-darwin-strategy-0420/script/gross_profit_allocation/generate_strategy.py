#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
"""
from ctypes import util
import sys
import json
import argparse
import logging
import requests
from datetime import datetime, timedelta
from main_strategy import Strategy
from utils import *

logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_parameter(config):
    param_str = config.parameter
    parsed = {}
    print("%s" % param_str)
    param = json.loads(param_str)
    parsed['is_online'] = param['is_online']
    parsed['trigger_time'] = param['trigger_time']
    parsed['trace_id'] = param['trace_id']
    parsed['order_id'] = param['order_id']
    parsed['budget'] = param['budget']  
    parsed['budget_limit'] = param['budget_limit']  
    parsed['cost'] = param['cost']
    parsed['operator'] = param['operator']
    parsed['child_info'] = param['child_info']
    parsed['rely_info'] = param['rely_info']
    parsed['start_date'] = param['start_date']
    parsed['end_date'] = param['end_date']
    parsed['step_start_date'] = param['step_start_date']
    parsed['step_end_date'] = param['step_end_date']
    parsed['step_id'] = param['step_id']
    parsed['step_type'] = param['step_type']
    parsed['version_list'] = param['version_list']
    parsed['ext_data'] = param['ext_data']
    return parsed


def stg_callback(api, data):
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
    stg_param = parse_parameter(args)
    logger.info('parsed parameters: %s' % str(stg_param))
    stg_param['cur_day'] = args.date
    stg = Strategy(config, stg_param, mode)
    print("mode: ", mode)
    # 0 B端分配
    # 1 调度分配
    # 2 呼返分配
    if(mode == 0):
        stg_param['title'] = "B端T+N分配"
        send_message(stg_param['title'],stg_param['is_online'], "B端分配开始")
        print("B端分配开始")
        stg_results = stg.b_budget_allocation()
    elif(mode == 1):
        stg_param['title'] = "调度T+N分配"
        send_message(stg_param['title'],stg_param['is_online'], "调度T+N分配开始")
        print("调度分配开始")
        stg_results = stg.dd_budget_allocation()
    elif(mode == 2):
        stg_param['title'] = "呼返T+1分配"
        send_message(stg_param['title'],stg_param['is_online'], "呼返T+1分配开始")
        print("C端分配开始")
        stg_results = stg.c_budget_allocation()
    else:
        print('mode error!')
        return
    
    # # 数据回传接口
    # stg_api_url = stg._api_url + "generalallocation/stgcallback"
    # print("callback stg url: ", stg_api_url)
    # print("callback stg result: ", stg_results)
    # resp = callback_request(stg_api_url, stg_results)
    # print('callback response: %s' % resp)
    # if int(resp.get('errno', -1)) != 0:
    #     print('Failed to callback with strategy result!!!')
    #     stg.error_callback('Failed to callback with strategy result!!!')
    # else:
    #     send_message(stg_param['title'],stg_param['is_online'], "分配已完成！")


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--config', help='配置文件', default='')
    arg_parser.add_argument('--date', help='活动计算日期，格式:yyyy-mm-dd format',
                            default=(datetime.datetime.now() + timedelta(days=0)).strftime('%Y-%m-%d'))
    arg_parser.add_argument('--parameter', help='活动参数', default='')
    arg_parser.add_argument('--res_cnt', help='生成结果数量', default=3)
    arg_parser.add_argument('--mode', help=' 区分业务线：bn, dispatch, c1', default=0)
    args = arg_parser.parse_args()

    main(args)
