"""资源分配系统执行入口。
目前支持 MPS、特区、专车、出租车 C 端 T+1 分配。
"""
import argparse
import datetime

import pandas as pd

# from loguru import logger
import const
from   policy.zhuan_policy import ZCFilterPolicy
from   policy.mps_policy import MPSFilterPolicy
from callback import post_process
from   data.data_access_object import DAO
from   policy.base_policy import run_policies
from resolver import load_conf, load_mps_conf_old_protocol, load_mps_conf_new_protocol
from solvers.base_solver import BaseSolver
from   policy.b_policy import generate_b_fillup_json, b_fillup_and_gererate_json
from callback import callback_request, error_callback, send_message, general_enlarge_rest_budget
from utils import json_to_dict, c_broadcast

def main(args):
    print("mode", args.mode)
    conf = json_to_dict(args.parameter)
    if args.mode == const.Mode.C and conf.stg_version == 'multi_target_v0':
        print("alloc mps...")
        conf = load_mps_conf_old_protocol(args)
    else:
        conf = load_conf(args)

    dao = DAO(conf)  # 读取data
    print("城市数量：", len(list(map(int, dao.cities.split(',')))))
    print("城市分框信息", dao.city_group_list)
    stg_api_url = dao._api_url + "generalallocation/stgcallback"
    conf.stg_api_url = stg_api_url
    """解析输入参数，执行资源分配"""
    if args.mode == const.Mode.C and conf.stg_version == 'multi_target_v0':
        conf.title = "双非呼返T+1分配"
        candidates = dao.get_mps_candidates()
        policies = [MPSFilterPolicy]
        """过滤候选集"""
        filter_candidates = run_policies(candidates, policies, dao, conf)
        print("candidate columns:", filter_candidates.columns)
        """城市分框求解"""
        resdf_list = []
        if len(dao.city_group_list) > 0:
            for group_info in dao.city_group_list:
                sdate, edate = group_info['start_date'], group_info['end_date']
                cis = group_info['city_list']
                if type(cis) == str:
                    cis = list(map(int, cis.split(',')))
                city_bucket_budget = group_info['budget_upperbound']
                # 整数规划求解
                opti_data = filter_candidates.query("city_id in @cis and stat_date >= '%s' and stat_date <= '%s'" % (sdate, edate))
                print("opti data number:", opti_data)
                if len(opti_data) == 0:
                    error_callback("城市分框的候选集为空！", conf)
                else:
                    solver = BaseSolver(conf, opti_data, is_bp=True, city_must_cost=True,
                                        mode='city_bucket', total_budget=city_bucket_budget, other_constraints=['pk_budget_upper_limit'])
                    resdf = solver.solve()
                    # 预算放缩
                    scale_cols = ['kc_subsidy_rate', 'th_subsidy_rate', 'pk_budget', 'th_budget', 'delta_cost']
                    resdf2 = general_enlarge_rest_budget(resdf, city_bucket_budget, scale_cols)
                    resdf_list.append(resdf2)
        out_put = pd.concat(resdf_list)
        print(out_put.head(5))
        resp = post_process(out_put, conf, dao, mode='city_bucket')
        # 对生效的t+1预算进行播报
        try:
            c_broadcast(out_put, conf['is_online'], conf['step_start_date'], conf['order_id'])
        except:
            error_callback('Failed to broadcast result !', conf)
    elif args.mode == const.Mode.B:
        conf.title = "B端T+N分配"
        gap_dignosis_all_strategy, fixed_threshold_list = dao.get_pregap_dignosis_info()
        pangu_elastic, acc_elastic = dao.get_elastic_b_hourly()
        forecast_daily_data = dao.get_forecast_data_for_b_fillup()
        asp_df = dao.get_asp_df()
        b_budget_list = b_fillup_and_gererate_json(fixed_threshold_list, gap_dignosis_all_strategy,
                                                   acc_elastic, pangu_elastic, forecast_daily_data,
                                                   asp_df,conf)
        stg_results = generate_b_fillup_json(b_budget_list,conf)
        print('stg_results', stg_results)
        resp = callback_request(stg_api_url, stg_results)
    elif args.mode == const.Mode.D:
        conf.title = "调度分配"
        pass
    elif args.mode == const.Mode.C:
        if conf.city_group == "zhuanche_normal":
            conf.title = "专车呼返T+1分配"
            candidates = dao.get_candidates("zhuanche_lgb")
            policies = [ZCFilterPolicy]
            candidates = run_policies(candidates, policies, dao, conf)
            solver = BaseSolver(conf, candidates)
            out_put = solver.solve()
            #out_put.to_csv('out_put.csv')
            print(out_put)
            resp = post_process(out_put,conf,dao)
        elif conf.city_group == "chuzuche_normal":
            conf.title = "出租车呼返T+1分配"
            candidates = dao.get_candidates("chuzu_lgb")
            solver = BaseSolver(conf, candidates,is_bp=False)
            out_put = solver.solve()
            print(out_put)
            resp = post_process(out_put,conf,dao)
        else:
            print(f'Unknown city_group : {conf.city_group }')
    else:
        print(f'Unknown mode: {args.mode}')
    
    print('conf_title:',conf.title)
    print(conf)
    if int(resp.get('errno', -1)) != 0:
        print('Failed to callback with strategy result!!!')
        error_callback('Failed to callback with strategy result!!!',conf)
    else:
        send_message(conf.title, conf.is_online, "分配已完成！")
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='配置文件', default='')
    default_date = datetime.datetime.now().strftime(const.Y_M_D)
    parser.add_argument('--date', help='活动计算日期，格式:yyyy-mm-dd format', default=default_date)
    parser.add_argument('--parameter', help='活动参数', default='', type=str)
    parser.add_argument('--res_cnt', help='生成结果数量', default=3, type=int)
    parser.add_argument('--mode', help=' 区分业务线：bn, dispatch, c1', default=0, type=int)
    args = parser.parse_args()
    main(args)