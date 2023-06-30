"""公用自定义函数"""
import json
from addict import Dict
import datetime
import calendar
import os
import logging
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
# from loguru import logger

C_WEBHOOK="https://im-dichat.xiaojukeji.com/api/hooks/incoming/8e56a5dd-dbaa-45b6-8339-47a30efe9b65"


class DChatRobot(object):
    """DChatRobot"""
    webhook = ""

    def __init__(self, webhook):
        super(DChatRobot, self).__init__()
        self.webhook = webhook

    def send_message(self, title, content_list, is_md=True):
        data = {"text": title, "markdown": is_md, "attachments": content_list}
        return self.post(data)

    def post(self, data):
        post_data = json.dumps({"web_hook": self.webhook, "data": data})
        print(post_data)
        HEADERS = {"Content-Type": "application/json ;charset=utf-8 "}
        req = requests.post("http://10.74.121.55:8021/stg/dchat_notification",
                            post_data,
                            headers=HEADERS)


def json_to_dict(s):
    """JSON str to dict which support dot access"""
    obj = json.loads(s)
    return Dict(obj)


def upload_to_gift(results, namespace, title, fmt='xlsx'):
    client = Minio(
        's3-gzpu.didistatic.com',
        access_key='AKDD00000000000VXSIXFN9EFRW5UM',
        secret_key='ASDDzpwZLeWuhaVaaSDdRzrJSVqLKlkxfnRWgPmY',
        secure=True,
    )
    filename = f'{namespace}/{datetime.datetime.now().strftime("%Y-%m-%d")}/{title}_{int(10000*random.random()):05d}.xlsx'
    client.fput_object('budget-overwrite', filename, results)
    print(f'https://s3-gzpu.didistatic.com/budget-overwrite/{filename}')
    return f'https://s3-gzpu.didistatic.com/budget-overwrite/{filename}'


def short_duration_check(start_time, end_time, b_rate):
    start_time = datetime.datetime.strptime(start_time, "%H:%M:%S")
    end_time = datetime.datetime.strptime(end_time, "%H:%M:%S")
    duration_hour = (end_time - start_time).seconds / 3600
    duration_check = 0
    if b_rate == 0.0 or b_rate >= 0.06:
        duration_check = 0
    elif duration_hour <= 3.0:  # 如果活动B补率在 0-6pp内，但是活动时长低于3小时
        duration_check = 1
    return duration_check


def allocation_info_daily_check(gap_b_table, daily_gmv_info):
    daily_gmv_info['pred_date'] = daily_gmv_info['stat_date']
    city_allocation_info = pd.merge(gap_b_table,
                                    daily_gmv_info,
                                    on=['city_id', 'pred_date'],
                                    how='left')
    city_allocation_info = city_allocation_info[[
        'city_id', 'pred_date', 'b_budget', 'acc_budget', 'gmv_daily'
    ]]
    city_allocation_info[
        'b_rate'] = city_allocation_info['b_budget'] / city_allocation_info['gmv_daily']
    city_allocation_info[
        'acc_rate'] = city_allocation_info['acc_budget'] / city_allocation_info['gmv_daily']
    print(city_allocation_info.head())

    # 城市补贴率信息明细
    _, output = tempfile.mkstemp(prefix='smt_', suffix='.xlsx')
    city_allocation_info.to_excel(output, index=None)
    b_allocation_daily_link = upload_to_gift(output, "预算分配报告", "b_allocation_daily_info", 'xlsx')

    return b_allocation_daily_link


def report_message(title,
                   env,
                   pic_info,
                   download_link,
                   abn_event_link,
                   gap_brate_event_link,
                   webhook=""):
    if webhook == "":
        webhook = "https://im-dichat.xiaojukeji.com/api/hooks/incoming/04b19429-d0a4-4475-986e-a1faa1bfa675"
        # webhook = "https://im-dichat.xiaojukeji.com/api/hooks/incoming/ac230690-40da-4123-85df-77ee6a07160b"
    robot = DChatRobot(webhook)
    if abn_event_link != "":
        abn_event_link = f'![这是图片]({abn_event_link})'
    if gap_brate_event_link != "":
        gap_brate_event_link = f'![这是图片]({gap_brate_event_link})'
    text_link = "" + abn_event_link + gap_brate_event_link
    robot.send_message(f'[**{pic_info}-{env}**]({download_link})', [{
        "title": title,
        "text": text_link,
        "color": "#cdff19"
    }], True)


def allocation_info_check(gap_choose_scheme, daily_gmv_info):
    """
    1、实现功能：
        a,处理异常分配结果，上传到gift，获取链接 
        b,绘制gap和坑深的关系图，这里面需要用不同颜色标出深坑，上传到gift，获取链接
        c,将整个结果上传到gift，获取链接 
    2、主要处理逻辑：通过处理好数据后，对不同数据不同处理，upload gift 操作
    """
    # check  shape
    allocation_df = pd.merge(gap_choose_scheme,
                             daily_gmv_info,
                             on=['city_id', 'stat_date'],
                             how='left')

    allocation_df = allocation_df[[
        'city_id', 'stat_date', 'fence_id', 'start_time', 'end_time', 'cr', 'cr_bar', 'b_rate',
        'subsidy', 'tool', 'gmv', 'gmv_daily'
    ]]
    allocation_df['b_rate_daily'] = allocation_df['subsidy'] / allocation_df['gmv_daily']
    print(allocation_df.head())

    total_df = allocation_df
    total_df['tool'] = total_df['tool'].apply(lambda x: "acc_card" if x == "加速卡" else "pangu")
    _, output = tempfile.mkstemp(prefix='smt_', suffix='.xlsx')
    total_df.to_excel(output, index=None)
    b_allocation_event_link = upload_to_gift(output, "预算分配报告", "b_allocation_event_info", 'xlsx')

    # 绘制gap和坑深之间的关系
    total_df["cr_gap_ori"] = total_df["cr_bar"] - total_df["cr"]
    total_df["duration"] = allocation_df.apply(
        lambda x: (datetime.datetime.strptime(x.end_time, '%H:%M:%S') - datetime.datetime.strptime(
            x.start_time, '%H:%M:%S')).total_seconds() / 3600.0,
        axis=1)
    pangu_df = total_df.query("tool == 'pangu' ")
    plt.scatter(pangu_df.cr_gap_ori, pangu_df.b_rate, c=pangu_df.duration)
    plt.colorbar()
    plt.title('The relation between gap and subsidy', fontsize=15, color='blue')
    #定义坐标轴
    plt.xlabel('gap_rate', fontsize=15, color='r')
    plt.ylabel('subsidy_rate', fontsize=15, color='r')

    _, output_pangu_gap = tempfile.mkstemp(prefix='smt_', suffix='.jpg')
    plt.savefig(output_pangu_gap)
    plt.close()
    gap_brate_event_link = upload_to_gift(output_pangu_gap, "预算分配报告", "gap_brate_relate_info",
                                          'jpg')

    # 获取异常补贴率活动的记录(all)
    abn_total_df = pangu_df.query(
        "(cr_gap_ori >= 0.06 and b_rate <= 0.05) or (cr_gap_ori <= 0.03 and b_rate >= 0.1) ")
    abn_total_df = abn_total_df[[
        'city_id', 'stat_date', 'fence_id', 'tool', 'start_time', 'end_time', 'cr_gap_ori', 'b_rate'
    ]]
    abn_total_df_head = abn_total_df.head(50)
    _, output_abn = tempfile.mkstemp(prefix='smt_', suffix='.jpg')
    dfi.export(abn_total_df_head,
               output_abn,
               fontsize=14,
               max_rows=None,
               max_cols=None,
               table_conversion='matplotlib',
               chrome_path=None)
    abn_event_link = upload_to_gift(output_abn, "预算分配报告", "abnormal_event_info", 'jpg')

    return b_allocation_event_link, gap_brate_event_link, abn_event_link, abn_total_df.shape[0]


def c_broadcast(df, is_online, pred_date, order_id):
    if df.shape[0] > 0:
        daily_info = df[['city_id', 'stat_date', 'ecr_bin', 'bin', 'score', 'min_combo_rate','max_combo_rate',
                         'kc_subsidy_rate', 'th_subsidy_rate', 'crate', 'pukuai_gmv', 'tehui_gmv']]
        daily_info = daily_info.rename(
            columns={'stat_date': 'tdate', 'bin': 'cr_bin', 'score': 'pred_cr', 'min_combo_rate': 'min_rate', 'max_combo_rate':'max_rate',
                     'kc_subsidy_rate': 'pkhf', 'th_subsidy_rate': 'thhf', 'crate': 'fkhf', 'pukuai_gmv': 'gmv_pk', 'tehui_gmv':'gmv_th'}
        )
        daily_info = daily_info.round({'pred_cr': 2, 'pkhf': 3, 'thhf': 3, 'fkhf': 3, 'gmv_pk': 1, 'gmv_th': 1})
        daily_info = daily_info.sort_values(by=['tdate', 'city_id'])

        # 剩余天规划的总体信息保存成excel
        _, output = tempfile.mkstemp(prefix='smt_', suffix='.xlsx')
        df.to_excel(output, index=None)
        print(output)
        c_allocation_budget_link = upload_to_gift(output, "预算分配报告", "c_allocation_budget_info", 'xlsx')
        # T+1 预算的图片展示
        daily_info_head = daily_info.sort_values('gmv_pk', ascending=False).head(50).reset_index(drop=True)
        _, output_daily_info = tempfile.mkstemp(prefix='smt_', suffix='.jpg')
        dfi.export(daily_info_head, output_daily_info, fontsize=14, max_rows=None, max_cols=None,
                   table_conversion='matplotlib', chrome_path=None)
        c_top50_budget_info_link = upload_to_gift(output_daily_info, "预算分配报告", "c_top50_budget_info", 'jpg')
        report_message("TOP50城C端预算如下", is_online, "[" + pred_date + "]-" + str(order_id) + "-C端呼返预算信息",
                       c_allocation_budget_link, c_top50_budget_info_link, "", webhook=C_WEBHOOK)
    else:
        print("No budget have been allocated")
    return