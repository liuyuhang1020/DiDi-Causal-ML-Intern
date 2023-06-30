import numpy as np
import pandas as pd
import sys
import os
import json
import argparse
import logging
import requests
from datetime import datetime, timedelta
from taocan import combo_strategy

logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_parameter(config):
    param_str = config.parameter
    parsed = {}
    print("%s" % param_str)
    # with open('%s'%param_str) as f:
    #    param = json.load(f)
    #    f.close()
    param = json.loads(param_str)
    parsed['is_online'] = param['is_online']
    parsed['trace_id'] = param['trace_id']
    parsed['order_id'] = param['order_id']
    parsed['city_list'] = param['city_list']
    parsed['budget'] = param['budget']  # fen
    parsed['operator'] = param['operator']
    parsed['start_date'] = param['start_date']
    parsed['end_date'] = param['end_date']
    parsed['remove_date'] = param['remove_date']
    parsed['zq_city'] = param['zq_city_list']
    parsed['amount_type'] = param['amount_type']
    parsed['city_gmv_list'] = param['city_gmv_list']
    parsed['trigger_time'] = param['trigger_time']
    parsed['task_type'] = param['task_type']
    parsed['ext_data'] = param['ext_data']
    if 'blackdict' in param:
        parsed['blackdict'] = param['blackdict']
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

def load_conf(conf_file):
    if not os.path.exists(conf_file):
        raise Exception('Failed to find config file: %s' % conf_file)
    with open(conf_file) as fconf:
        config = json.load(fconf)
        logger.info('config: %s' % config)
        return config

def main(args):
    # 解析json文件
    config = load_conf(args.config)
    if args.parameter == '':
        return
    # 解析输入参数
    stg_param = parse_parameter(args)
    logger.info('parsed parameters: %s' % str(stg_param))
    stg_param['cur_day'] = args.date
    stg = combo_strategy(config, stg_param)
    stg_results = stg.gen_output()

    if stg_param['is_online'] == "online":
        api_url = config['stg_callback_api']
    elif stg_param['is_online'] == "pre":
        api_url = config['stg_callback_api_pre']
    elif stg_param['is_online'] == "test":
        api_url = config['stg_callback_api_test']
    print("finish to generate strategy, result: ", api_url)
    # print(stg_results)
    resp = stg_callback(api_url, stg_results)
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
    # arg_parser.add_argument('--mode', help='xman or shumeng', default=-1)
    args = arg_parser.parse_args()

    main(args)

