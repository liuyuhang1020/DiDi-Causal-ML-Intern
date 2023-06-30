#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
"""
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import json
import argparse
import logging
import requests
from datetime import datetime, timedelta

from c_strategy import Strategy
from utils import load_conf, execute_cmd, execute_cmd_and_get_result, time_to_float_hour, hdfs_path_exist

logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_parameter(param_str):
    parsed = {}
    print("%s"%param_str)
    #with open('%s'%param_str) as f:
    #    param = json.load(f)
    #    f.close()
    param = json.loads(param_str)
    #'''
    parsed['is_online'] = param['is_online']
    parsed['trace_id'] = param['trace_id']
    parsed['order_id'] = param['order_id']
    parsed['start_date'] = param['start_date']
    parsed['end_date'] = param['end_date']
    parsed['remove_date'] = param['remove_date']
    parsed['city_list'] = param['city_list']
    parsed['budget'] = param['budget'] # fen
    parsed['operator'] = param['operator']
    parsed['trigger_time']= param['trigger_time']
    #'''
    if 'request' in param:
        stg_param = json.loads(param['request'])
        logger.info('parameters %s '%str(param))
        key = None
        try:
            key = 'budget'
            parsed[key] = int(stg_param['budget'])

            key = 'trace_id'
            parsed[key] = stg_param['trace_id']
            key = 'order_id'
            parsed[key] = int(stg_param['order_id'])
            key = 'is_online'
            parsed[key] = stg_param['is_online']

            key = 'start_date'
            parsed[key] = stg_param['start_date']
            key = 'end_date'
            parsed[key] = stg_param['end_date']
            key = 'remove_date'
            parsed[key] = stg_param['remove_date']
            key = 'city_list'
            parsed[key] = stg_param['city_list']
            key = 'operator'
            parsed[key] = stg_param['operator']
            key = 'trigger_time'
            parsed['trigger_time'] = stg_param['trigger_time']

        except:
            raise Exception('Failed to parse %s from input parameter'%key)
    return parsed

def stg_callback(api, data):
    headers = {'Content-Type': 'application/json;charset=UTF-8'}
    headers = {'Content-Type':'application/x-www-form-urlencoded'}
    #resp = requests.post(api, data=data, headers=headers, timeout=None)

    resp = requests.post(api, data=data, timeout=None)
    if resp.status_code != requests.codes.ok:
        raise Exception('%s : %s'%(api, resp.text))
    return json.loads(resp.text)

def main(args):
    config = load_conf(args.config)
    if args.parameter == '':
        return
    stg_param = parse_parameter(args.parameter)
    logger.info('parsed parameters: %s'%str(stg_param))
    stg = Strategy(config, stg_param)
    stg_results = stg.generate()
    '''
    if stg_param['is_online'] == True:
        api_url = config['stg_callback_api']
    elif stg_param['is_online'] == False:
        api_url = config['stg_callback_api_pre']
    else:
        api_url = config['stg_callback_api_test']
    print("finish to generate strategy, result: %s"%stg_results)
    resp = stg_callback(api_url, stg_results)
    print('callback response: %s'%resp)
    if int(resp.get('errno', -1)) != 0:
        raise Exception('Failed to callback with strategy result!!!')
    '''



if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--config', help='配置文件', default='')
    arg_parser.add_argument('--date', help='活动计算日期，格式:yyyy-mm-dd format',
                            default=(datetime.now() + timedelta(days=0)).strftime('%Y-%m-%d'))
    arg_parser.add_argument('--parameter', help='活动参数', default='')
    arg_parser.add_argument('--res_cnt', help='生成结果数量', default=3)
    args = arg_parser.parse_args()

    main(args)