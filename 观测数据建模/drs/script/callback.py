"""回调资源分配下游接口，存储日志"""
import json
from addict import Dict
import requests
# from loguru import logger
from utils import DChatRobot
import pandas as pd
import const

def post_process(out_put, CONF, dao, mode='all'):
    if mode != 'city_bucket':  # 是否城市分框
        out_put = enlarge_rest_budget(out_put, CONF)
        print('enlarge_output:', out_put)
    else:
        print("city bucket  scale operation before posting...")
    stg_detail = generate_output_c(out_put, CONF, dao)
    print('stg_detail:', stg_detail)
    resp = callback_request(CONF.stg_api_url, stg_detail)
    return resp

def log_to_hdfs():
    pass
        
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

def callback_request(api, data):
    # headers = {'Content-Type': 'application/json;charset=UTF-8'}
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    resp = requests.post(api, data=data, headers=headers, timeout=None)
    # print("*******返回结果测试，resp*******",resp.json()) 
    if resp.status_code != requests.codes.ok:
        print("****************返回结果测试 **********",resp.status_code)
        raise Exception('%s : %s' % (api, resp.text))
    return json.loads(resp.text)

def error_callback(errmsg, CONF):
    send_message(CONF.title, CONF.is_online, errmsg )
    error_msg = {
        "err_no": 1,
        "err_msg": errmsg,
        "order_id": CONF.order_id,
        "trace_id": CONF.trace_id,
        "step_id": CONF.step_id
    }
    stg_api_url =  CONF.api_url + "generalallocation/stgcallback"
    print("callback stg url: ", stg_api_url)
    resp = callback_request(stg_api_url, error_msg)
    print('callback response: %s' % resp)
    raise Exception(errmsg)

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

def general_enlarge_rest_budget(choose_scheme, budget, scale_cols):
    rest_money = choose_scheme['delta_cost'].sum()
    enlarge_times = budget / rest_money
    print('enlarge_times:', enlarge_times)
    choose_scheme[scale_cols] = choose_scheme[scale_cols].apply(lambda x: x * enlarge_times, axis=1)
    return choose_scheme

def enlarge_rest_budget(choose_scheme, CONF):
    print(CONF)
    rest_money = choose_scheme['delta_cost'].sum()
    enlarge_times = CONF.budget_limit/rest_money
    print('enlarge_times:',enlarge_times)
    choose_scheme[['pk_hufan_rate','th_hufan_rate','zhuan_hufan_rate','chuzu_hufan_rate','delta_cost']] = choose_scheme[['pk_hufan_rate','th_hufan_rate','zhuan_hufan_rate','chuzu_hufan_rate','delta_cost']].apply(lambda x: x * enlarge_times,axis = 1)
    return choose_scheme

def generate_output_c(choose_scheme, CONF, dao):
    temp_df = choose_scheme[choose_scheme['stat_date'] == CONF.step_start_date]
    print("T+1 预算总计:{}".format(temp_df["delta_cost"].sum()))
    if CONF.city_group == "zhuanche_normal":
        reslist = cal_zhuan_detail(temp_df,CONF,dao)
    if CONF.city_group == "mps":
        reslist = cal_mps_detail(temp_df, CONF)
    if CONF.city_group == "chuzuche_normal":
        reslist = cal_chuzu_detail(temp_df,CONF)
    version_info = []
    version_info_sub = {
    "rely_info" : CONF.rely_info,
        "version": "",
        "data":reslist,
        "ext_data": {"sum_cr_rate": {}, "delta_gmv": 0.0}
    }   
    version_info.append(version_info_sub)
    resp = {
        "order_id": CONF.order_id,
        "step_id": CONF.step_id,
        "trace_id": CONF.trace_id,
        "version_info": json.dumps(version_info)
    }
    return resp

def cal_zhuan_detail(temp_df, CONF, dao):
    nor_zhuan_ratio_df = dao.get_nor_zhuan_rate()
    temp_df = pd.merge(temp_df,nor_zhuan_ratio_df,on = ['city_id'],how = 'left')
    temp_df['nor_zhuan_ratio'] = temp_df['nor_zhuan_ratio'].fillna('0.7')
    temp_df['nor_delta_cost'] = temp_df['nor_zhuan_ratio'] * temp_df['delta_cost']
    temp_df['cyd_delta_cost'] = temp_df['delta_cost'] - temp_df['nor_delta_cost']
    reslist = []
    for city_id in set(CONF.cities.split(",")):
        city_df = temp_df.query('city_id == {}'.format(city_id))
        for _,row in city_df.iterrows():
            # 橙意单
            c_c_json = {}
            c_c_json["stat_date"] = row.stat_date
            c_c_json["city"] = row.city_id
            c_c_json["product_line"] = "zhuanche"
            c_c_json["caller"] = "dape-aps"
            c_c_json["gmv"] = float(row.zhuanche_gmv)
            c_c_json["step_type"] = "hufan_T_1"
            c_c_json["stg_detail"] = []
            c_c_json["amount"] = float(row.cyd_delta_cost)
            # 普通单
            c_n_json = c_c_json.copy()
            c_n_json['amount'] = float(row.nor_delta_cost)
            c_n_json["product_line"] = "fake_zhuanche"
            reslist.append(c_c_json)
            reslist.append(c_n_json)
    return reslist

def cal_chuzu_detail(temp_df, CONF):
    reslist = []
    for city_id in set(CONF.cities.split(",")):
        city_df = temp_df.query('city_id == {}'.format(city_id))
        for _,row in city_df.iterrows():
            c_c_json = {}
            c_c_json["stat_date"] = row.stat_date
            c_c_json["city"] = row.city_id
            c_c_json["product_line"] = "chuzuche"
            c_c_json["caller"] = "dape-aps"
            c_c_json["gmv"] = float(row.chuzuche_gmv)
            c_c_json["step_type"] = "hufan_T_1"
            c_c_json["stg_detail"] = []
            c_c_json["amount"] = float(row.delta_cost)
            reslist.append(c_c_json)
    return reslist

def cal_mps_detail(temp_df, CONF):
    reslist = []
    for city_id in set(CONF.cities.split(",")):
        city_df = temp_df.query('city_id == {}'.format(city_id))
        for _, row in city_df.iterrows():
            c_k_json = {}
            c_th_json = {}
            c_k_json["stat_date"] = row.stat_date
            c_k_json["city"] = row.city_id
            c_k_json["product_line"] = "kuaiche"
            c_k_json["caller"] = "dape-aps"
            c_k_json["gmv"] = row.pukuai_gmv
            c_k_json["step_type"] = "hufan_T_1"
            c_k_json["stg_detail"] = []
            c_k_json["amount"] = row.pk_budget

            c_th_json["stat_date"] = row.stat_date
            c_th_json["city"] = row.city_id
            c_th_json["product_line"] = "tehui"
            c_th_json["caller"] = "dape-aps"
            c_th_json["gmv"] = row.tehui_gmv
            c_th_json["step_type"] = "hufan_T_1"
            c_th_json["stg_detail"] = []
            c_th_json["amount"] = row.th_budget

            reslist.append(c_k_json)
            reslist.append(c_th_json)
    return reslist