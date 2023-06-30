import json

import pandas as pd
import requests
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

webhook = 'https://im-dichat.xiaojukeji.com/api/hooks/incoming/c64821f6-36b7-4a5d-8375-f740ac63b448'

dapan_sql = '''
select
  *
from smt_stg.smt_budget_alloc_dapan_t2
where
  dt = '{dt}'
'''

city_sql = '''
select 
  city_id,
  city_name,
  level_1_bucket gmv_type
from mp_data.dim_trip_mp_city_di
where dt = '{dt}'
'''

gmv_sql = '''
select
  city_id,
  sum(case when product_id = 110103  then gmv else 0.0 end ) as th_gmv,
  sum(case when product_id = 110101  then gmv else 0.0 end ) as pk_gmv
from mp_data.dm_trip_mp_sd_core_1d
where
  dt = '{dt}'
  and product_id in (110101, 110103)
group by city_id
'''

pred_sql = '''
select
  a.city_id city_id,
  pred_gmv,
  pred_obj_cr,
  pred_pk_gmv,
  pred_th_gmv,
  pred_exp_cr
from (
select  city_id
        ,sum(if(metric = 'gmv', forecast_value, 0)) as pred_gmv
        ,sum(if(metric = 'objective_exp_openapi_pp', forecast_value, 0)) as pred_obj_cr
        ,sum(if(metric = 'exp_cr_exp_openapi', forecast_value, 0)) as pred_exp_cr
from    mp_data.dm_trip_mkt_supply_demand_forecast_di
where   forecast_date = '{estimate_date}'
        and datediff(forecast_date, dt) = {diff}
        and product_id = '110100_110400_110800_110103'
group by city_id
) a join (
select  city_id
        ,sum(if(product_name = '普通快车' and metric = 'gmv', forecast_value, 0)) as pred_pk_gmv
        ,sum(if(product_name = '特惠自营' and metric = 'gmv', forecast_value, 0)) as pred_th_gmv
from    mp_data.dm_trip_mkt_supply_demand_forecast_di
where   forecast_date = '{estimate_date}'
        and datediff(forecast_date, dt) = {diff}
        and product_name in ('普通快车', '特惠自营')
group by city_id
) b on a.city_id = b.city_id
'''

pred_roi_sql = '''
select
    city_id,
    pred_roi,
    combo_subsidy_rate
from smt_stg.budget_allocation_pred_delta_gmv_v2
where 
    concat_ws('-', year, month, day)='{dt}' 
    and version = 'roi_lgb_v1'
'''
real_roi_sql = '''
select
    exp.city_id as city_id
    ,(exp.avg_gmv - ctl.avg_gmv)/(exp.avg_c - ctl.avg_c) as real_roi
from
    (select
        city_id,
        gmv/group_usr_num as avg_gmv,
        subsidy_c/group_usr_num as avg_c
    from smt_stg.budget_allocation_xfk_city_daily_table
    where group_name='rgroup_joint_exp_pack'
          and product_line = '新泛快'
          and dt = '{dt}'
    ) exp join
    (select
        city_id,
        gmv/group_usr_num as avg_gmv,
        subsidy_c/group_usr_num as avg_c
    from smt_stg.budget_allocation_xfk_city_daily_table
    where group_name='rgroup_joint_blank'
          and product_line = '新泛快'
          and dt = '{dt}'
    ) ctl on exp.city_id = ctl.city_id
'''

exp_hf_sql = '''
select
  a.city as city_id,
  exp_pkhf,
  exp_thhf
from
(select
    dt --日期
    , city --城市
    , JSON_EXTRACT(target_info, '$.rate') as exp_pkhf
from
    ppe.budget_amount_record
where
    dt = '{dt}'
    and amount_type='exp_budget'
    and  tuple_info like '{group}'
    and product_line = 'kuaiche'
) a join
(select
    dt --日期
    , city --城市
    , JSON_EXTRACT(target_info, '$.rate') as exp_thhf
from
    ppe.budget_amount_record
where
    dt = '{dt}'
    and amount_type='exp_budget'
    and  tuple_info like '{group}'
    and product_line = 'tehui'
) b on a.dt = b.dt and a.city = b.city
'''

kt_ratio_sql = '''
select
 a.city_id city_id,
 avg_th,
 avg_pk,
 avg_th/avg_pk as kt_ratio
from (
select
		city_id
        ,(sum(gmv) - sum(subsidy_c))/sum(order_charge_dis) as avg_pk
from	mp_data.dwm_trip_trd_psg_core_di
where   dt = '{dt}'
        and product_line = '普通快车非拼'
group by city_id
) a join (
select
		city_id
        ,(sum(gmv) - sum(subsidy_c))/sum(order_charge_dis) as avg_th
from	mp_data.dwm_trip_trd_psg_core_di
where   dt = '{dt}'
        and product_line = '特惠自营'
group by city_id
) b on a.city_id = b.city_id
'''

exp_cr_sql = '''
select
  a.city_id,
  a.dt,
  pred_exp_cr,
  exp_cr,
  pred_exp_cr - exp_cr cr_diff,
  gmv_type,
  city_name
from (
select  
    cast(city_id as Integer) city_id
    ,cast(nvl(finish_rate, 0) as Double) pred_exp_cr
    ,dt
from    
    riemann.gongxu_cr_half_hour_pred_test --体验cr临时表
where   
    exp_id = 'irving'
    and product_id = 'day_110100_110400_110800_110103'
    and concat_ws('-', year, month, day) between date_sub('{start_dt}', 1) and date_sub('{end_dt}', 1)
) a join (
select
  city_id,
  dt,
  exp_cr
from smt_stg.smt_budget_alloc_dapan_t2
where
  dt between '{start_dt}' and '{end_dt}'
) b on a.city_id = b.city_id and a.dt = b.dt join (
select 
  city_id,
  city_name,
  gmv_type
from smt_stg.smt_mps_city_info
) c on a.city_id = c.city_id
'''

insert_sql = '''
insert overwrite table
    smt_stg.smt_err_budget_alloc_sum_t1
partition
    (dt = '{dt}')
select
    source_type,
    err_type,
    city_cnt,
    gmv,
    fkhf_amount,
    mid_cr,
    mid_fkhf,
    city_total,
    gmv_total,
    fkhf_total
from {tmp}
'''

insert_city_sql = '''
insert overwrite table
    smt_stg.smt_err_budget_alloc_city_t1
partition
    (dt = '{dt}')
select
    city_id,
    city_name,
    city_type,
    gmv_type,
    demand,
    source_type,
    err_type,
    gmv,
    exp_cr,
    fkhf,
    pred_gmv,
    target_fkhf,
    obj_cr,
    pred_obj_cr,
    cr_diff,
    fkhf_diff,
    diag1,
    diag2,
    diag3,
    diag4,
    pred_roi,
    real_roi,
    obj_cr_diff,
    pred_obj_cr_diff,
    algo_fkhf,
    algo_fkhf_diff,
    algo_err_type,
    case_level,
    is_manual,
    pred_exp_cr,
    pred_exp_cr_diff,
    kt_ratio,
    exp_cr_diag
from {tmp}
'''


class DChatRobot:
    """DChatRobot"""

    def __init__(self, webhook):
        super(DChatRobot, self).__init__()
        self.webhook = webhook

    def send_message(self, title, content_list, is_md=True):
        data = {
            "text": title,
            "markdown": is_md,
            "attachments": content_list
        }
        return self.post(data)

    def post(self, data):
        post_data = json.dumps({
            "web_hook": self.webhook,
            "data": data
        })
        print(post_data)
        HEADERS = {"Content-Type": "application/json ;charset=utf-8 "}
        req = requests.post("http://10.74.113.54:8021/stg/dchat_notification", post_data,
                            headers=HEADERS)


def get_badcase_by_mean(df, by_category=True, hf_col='', diag_col=''):
    gmv_types = ['']
    if by_category:
        gmv_types = [('特大城市', '大城市', '中城市'), ('小城市', '尾部城市'), ('东南', '中南', '北区', '西南'), ('战区',)]
    dfs = []
    for g_type in gmv_types:
        df1 = df[df.gmv_type.isin(g_type)] if by_category else df.copy()
        df1 = df1.reset_index(drop=True)
        m_hf_col = 'm_' + hf_col
        hf_diff_col = hf_col + '_diff'
        df1[m_hf_col] = df1[hf_col].median()
        df1['m_cr'] = df1.exp_cr.median()
        df1[hf_diff_col] = (df1[hf_col] - df1[m_hf_col]) / df1[m_hf_col]
        df1['cr_diff'] = (df1.exp_cr - df1.m_cr) / df1.m_cr
        # add more cr diff
        df1['m_obj_cr'] = df1.obj_cr.median()
        df1['obj_cr_diff'] = (df1.obj_cr - df1.m_obj_cr) / df1.m_obj_cr
        df1['m_pred_obj_cr'] = df1.pred_obj_cr.median()
        df1['pred_obj_cr_diff'] = (df1.pred_obj_cr - df1.m_pred_obj_cr) / df1.m_pred_obj_cr
        df1['m_pred_exp_cr'] = df1.pred_exp_cr.median()
        df1['pred_exp_cr_diff'] = (df1.pred_exp_cr - df1.m_pred_exp_cr) / df1.m_pred_exp_cr
        df1[diag_col] = '正常'
        for i, row in df1.iterrows():
            dcr, dhf = row.cr_diff, row[hf_diff_col]
            diag = '正常'
            if dcr > 0.03 and dhf < -0.0 and row.real_roi > 1:
                diag = '严重漏补'
            elif dcr > 0.0 and dhf < -0.5 and row.real_roi > 1:
                diag = '中等漏补'
            elif dcr > 0.0 and dhf < -0.05 and row.real_roi > 1:
                diag = '轻度漏补'
            elif dcr < -0.05 and row[hf_col] > 0.01:
                diag = '严重误补'
            elif dcr < -0.0 and dhf > 0.5:
                diag = '中等误补'
            elif dcr < -0.0 and dhf > 0.05:
                diag = '轻度误补'
            row[diag_col] = diag
            df1.iloc[i] = row
        dfs.append(df1)
    odf = pd.concat(dfs)
    return odf


def cr_diag(row):
    pred_diff = row.pred_obj_cr - row.obj_cr
    if '误补' in row.err_type or '误补' in row.algo_err_type:
        if pred_diff > 0.07:
            row.diag1 = 1
        if pred_diff > 0.05:
            row.diag2 = 1
        if pred_diff > 0.03:
            row.diag3 = 1
        if (row.obj_cr < 0.72 and row.pred_obj_cr > 0.72 and pred_diff > 0.03) or (
                row.obj_cr > 0.72 and pred_diff > 0.05):
            row.diag4 = 1
    elif '漏补' in row.err_type or '漏补' in row.algo_err_type:
        if pred_diff < -0.07:
            row.diag1 = 1
        if pred_diff < -0.05:
            row.diag2 = 1
        if pred_diff < -0.03:
            row.diag3 = 1
        if (row.obj_cr > 0.8 and row.pred_obj_cr < 0.8 and pred_diff < -0.04) or (
                row.obj_cr < 0.8 and pred_diff < -0.05):
            row.diag4 = 1
    return row

def check_exp_cr(df):
    """体验 CR 诊断"""
    df = df.sort_values('dt', ascending=False)
    cr_diffs = df['cr_diff'].to_list()
    gmv_type = df.iloc[0, :].gmv_type
    if gmv_type in ('特大城市', '大城市', '中城市'):
        limit1 = 0.03
        limit2 = 0.02
    else:
        limit1 = 0.05
        limit2 = 0.03
    if abs(cr_diffs[0]) > limit1 or all(abs(x) > limit2 for x in cr_diffs):
        df['exp_cr_diag'] = 1
    else:
        df['exp_cr_diag'] = 0
    return df[['city_id', 'exp_cr_diag']].iloc[0, :]


def get_case_level(row):
    if row.gmv_type == '大城市' and '严重' in row.err_type:
        row.case_level = 'P0'
    elif (row.gmv_type == '大城市' and '中等' in row.err_type) or (
            row.gmv_type == '中城市' and '严重' in row.err_type):
        row.case_level = 'P1'
    elif '正常' in row.err_type:
        row.case_level = '/'
    else:
        row.case_level = 'P2'
    return row


def main():
    spark = SparkSession.builder.appName(
        'pukuai_tehui_zhuanche_dgmv_pred daily inference').enableHiveSupport().getOrCreate()
    spark.conf.set('spark.sql.broadcastTimeout', 360000)
    spark.conf.set('hive.exec.dynamic.partition.mode', 'nonstrict')
    spark.sql('set spark.sql.hive.convertMetastoreOrc=true')
    spark.sql('set spark.sql.orc.impl=native')
    spark.sql('set dfs.client.socket-timeout=600000')
    sc = spark.sparkContext
    hc = HiveContext(sc)

    yesterday = '${BIZ_DATE_LINE}'
    df = hc.sql(dapan_sql.format(dt=yesterday)).toPandas()
    city_df = hc.sql(city_sql.format(dt=yesterday)).toPandas()
    df = pd.merge(df, city_df, on='city_id', how='left')
    for i in range(1, 3):
        pred_df = hc.sql(pred_sql.format(estimate_date=yesterday, diff=i)).toPandas()
        if not pred_df.empty:
            break
    df = pd.merge(df, pred_df, how='left', on='city_id')
    gmv_df = hc.sql(gmv_sql.format(dt=yesterday)).toPandas()
    df = pd.merge(df, gmv_df, how='left', on='city_id')
    kt_ratio_df = hc.sql(kt_ratio_sql.format(dt=yesterday)).toPandas()
    df = pd.merge(df, kt_ratio_df, how='left', on='city_id')
    df = df.fillna('0').astype({
        'gmv': 'float',
        'exp_cr': 'float',
        'intelligent_subsidy_c': 'float',
        'real_thhf': 'float',
        'real_pkhf': 'float',
        'algo_thhf': 'float',
        'algo_pkhf': 'float',
        'obj_cr': 'float',
        'pred_obj_cr': 'float',
        'pred_exp_cr': 'float',
        'pred_pk_gmv': 'float',
        'pred_th_gmv': 'float',
        'pred_gmv': 'float',
        'pk_gmv': 'float',
        'th_gmv': 'float',
        'kt_ratio': 'float',
    })
    real_roi_df = hc.sql(real_roi_sql.format(dt=yesterday)).toPandas()
    df = pd.merge(df, real_roi_df, how='left', on='city_id')
    df['real_roi'] = df.real_roi.fillna(0)
    df['fkhf'] = df.intelligent_subsidy_c / df.gmv

    df['target_fkhf'] = (df.pred_pk_gmv * df.real_pkhf + df.pred_th_gmv * df.real_thhf) / df.pred_gmv
    df['algo_fkhf'] = (df.pred_pk_gmv * df.algo_pkhf + df.pred_th_gmv * df.algo_thhf) / df.pred_gmv
    # 判断 bad case
    df = get_badcase_by_mean(df, True, 'algo_fkhf', 'algo_err_type')
    df = get_badcase_by_mean(df, True, 'fkhf', 'err_type')

    agg_df = df.groupby('err_type').agg({
        'city_id': 'count',
        'gmv': 'sum',
        'intelligent_subsidy_c': 'sum',
        'exp_cr': 'median',
        'fkhf': 'median',
    }).reset_index()
    agg_df = agg_df.rename(columns={
        'city_id': 'city_cnt',
        'intelligent_subsidy_c': 'fkhf_amount',
        'exp_cr': 'mid_cr',
        'fkhf': 'mid_fkhf',
    })
    agg_df['source_type'] = '后验'
    agg_df['city_total'] = df.city_id.count()
    agg_df['gmv_total'] = df.gmv.sum()
    agg_df['fkhf_total'] = df.intelligent_subsidy_c.sum()
    # 总体统计
    tmp_name = 'tmp_table'
    spark.createDataFrame(agg_df[[
        'source_type', 'err_type', 'city_cnt', 'gmv', 'fkhf_amount', 'mid_cr', 'mid_fkhf',
        'city_total', 'gmv_total', 'fkhf_total'
    ]].copy()).registerTempTable(tmp_name)
    hc.sql(insert_sql.format(dt=yesterday, tmp=tmp_name))

    # 分城市统计
    df = df.astype({
        'pred_obj_cr': 'float',
    })
    df['source_type'] = '后验'
    df['diag1'] = 0
    df['diag2'] = 0
    df['diag3'] = 0
    df['diag4'] = 0
    df = df.apply(cr_diag, axis=1)

    # 体验 CR 诊断
    # start_day = (datetime.strptime(yesterday, '%Y-%m-%d') - timedelta(days=3)).strftime('%Y-%m-%d')
    # params = {
    #     'start_dt': start_day,
    #     'end_dt': yesterday,
    # }
    # exp_cr_df = hc.sql(exp_cr_sql.format(**params)).toPandas()
    # exp_cr_diag = exp_cr_df.groupby('city_id').apply(check_exp_cr).reset_index(drop=True)
    # df = pd.merge(df, exp_cr_diag, how='left', on='city_id')
    df['exp_cr_diag'] = 0

    pred_roi_df = hc.sql(pred_roi_sql.format(dt=yesterday)).toPandas()
    rates_df = pred_roi_df['combo_subsidy_rate'].str.split(',', expand=True)
    pred_roi_df['algo_pkhf'] = rates_df[0].astype('float')
    pred_roi_df['algo_thhf'] = rates_df[1].astype('float')
    df['algo_pkhf'] = df.algo_pkhf.round(3)
    df['algo_thhf'] = df.algo_thhf.round(3)
    df = pd.merge(df, pred_roi_df, how='left', on=['city_id', 'algo_pkhf', 'algo_thhf'])
    df['pred_roi'] = df.pred_roi.fillna(0)
    df['case_level'] = ''
    df = df.apply(get_case_level, axis=1)
    df['is_manual'] = abs(df.fkhf - df.algo_fkhf) > 0.5
    df = df.astype({'is_manual': 'int'})

    tmp_name = 'tmp_table1'
    spark.createDataFrame(df[[
        'city_id', 'city_name', 'city_type', 'gmv_type', 'demand', 'source_type', 'err_type', 'gmv',
        'exp_cr', 'fkhf', 'pred_gmv', 'target_fkhf', 'obj_cr', 'pred_obj_cr', 'cr_diff',
        'fkhf_diff', 'diag1', 'diag2', 'diag3', 'diag4', 'pred_roi', 'real_roi', 'obj_cr_diff',
        'pred_obj_cr_diff', 'algo_fkhf', 'algo_fkhf_diff', 'algo_err_type', 'case_level',
        'is_manual', 'pred_exp_cr', 'pred_exp_cr_diff', 'kt_ratio', 'exp_cr_diag'
    ]].copy()).registerTempTable(tmp_name)
    hc.sql(insert_city_sql.format(dt=yesterday, tmp=tmp_name))

    # DC 报警
    df['dt'] = yesterday
    df = df[['dt', 'city_id', 'city_name', 'gmv_type', 'algo_err_type', 'gmv', 'exp_cr',
             'pred_exp_cr', 'algo_fkhf', 'case_level']]
    df['gmv'] = df.gmv / 10000
    df = df.astype({'gmv': 'int'}).astype({'gmv': 'str'})
    df['gmv'] = df.gmv + 'W'
    df['exp_cr'] = round(df.exp_cr, 3)
    df['pred_exp_cr'] = round(df.pred_exp_cr, 3)
    df['algo_fkhf'] = round(df.algo_fkhf * 100, 1)
    gmv_types = ['大城市', '中城市']
    err_types = ['正常']
    df = df[(df.gmv_type.isin(gmv_types)) & (~df.algo_err_type.isin(err_types))]
    df = df.sort_values('city_id')
    df = df.rename(columns={
        'dt': '日期',
        'city_id': '城市ID',
        'city_name': '城市名',
        'gmv_type': '城市类型',
        'algo_err_type': '算法分配异常分类',
        'gmv': '泛快GMV',
        'exp_cr': '体验成交率',
        'pred_exp_cr': '预估体验成交率',
        'algo_fkhf': '算法分配泛快呼返(%)',
        'case_level': 'case 等级',
    })
    lines = ['\t'.join(r for r in df.columns.tolist())]
    for i, row in df.iterrows():
        lines.append('\t'.join(str(r) for r in row))
    text = '\n'.join(lines)
    title = 'MPS 呼返异常补贴报警（大盘整体）'
    robot = DChatRobot(webhook)
    robot.send_message(title, [{'text': text}])
    return df


if __name__ == '__main__':
    main()
