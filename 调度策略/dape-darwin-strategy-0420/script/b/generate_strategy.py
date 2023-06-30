import json
import argparse
import logging
import requests
from datetime import datetime, timedelta
import toml
from strategy import Strategy

logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_parameter(param_str):
    parsed = {}
    param = json.loads(param_str)
    logger.info(f'parameters: {param}')
    key = None
    try:
        parsed = {"is_online": param['is_online'],
                  "trace_id": str(param['trace_id']),
                  "order_id": int(param['order_id']),
                  "start_date": str(param['start_date']),
                  "end_date": str(param['end_date']),
                  "remove_date": param['remove_date'],
                  "city_list": param['city_list'],
                  "budget": int(param['budget']),
                  "operator": str(param['operator']),
                  "trigger_time": str(param['trigger_time'])}
        
    except:
        raise Exception(f'Failed to parse {key} from input parameter')
    return parsed


def main(args):
    with open(args.config) as f:
        config = toml.load(f)
    if args.parameter == '':
        return
    stg_param = parse_parameter(args.parameter)
    logger.info(f'parsed parameters: {stg_param}')
    stg = Strategy(config, args.date, stg_param, args.diagnosis_date)
    stg_results = stg.generate()
    print(stg_results)
    
    


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--config', help='配置文件', default='')
    arg_parser.add_argument('--date', help='活动生效日期，格式:yyyy-mm-dd format',
                            default=(datetime.now()).strftime('%Y-%m-%d'))
    arg_parser.add_argument('--parameter', help='活动参数', default='')
    arg_parser.add_argument('--diagnosis_date', help='date', default=(datetime.now().strftime("%Y-%m-%d")))
    args = arg_parser.parse_args()
    main(args)