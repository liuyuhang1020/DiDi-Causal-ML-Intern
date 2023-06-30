# coding=utf-8
"""Data access object for allocation."""
import time
# from loguru import logger
from const import C, B, max_try_num, MIN_NUM_OF_DATA
from callback import error_callback, get_b_rate_dict
from data.hive_sql import *
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import json
import math

conf_to_cate = {'hufan_T_1': C.fk, 'b_decision': B.zc}
# 品类映射到供需预测表
cate_to_gx = {
    C.fk: '泛快含优不含拼（快车（普通快车、A+、车大、巡游网约车）、优享、D1、特惠快车、涅槃）',
}
# 品类映射到资源分配预估结果表
cate_to_pred = {C.fk: 'fankuai', C.zc: 'zhuanche_lgb'}


class DAO:

    def __init__(self, conf):
        assert conf.step_type in conf_to_cate
        self.cate = conf_to_cate[conf.step_type]
        self.start_date = conf.start_date  # 预算分配开始日期
        self.end_date = conf.end_date  # 预算分配结束日期
        self.step_start_date = conf.step_start_date  # c端t+1预算分配生效日期
        self.cur_day = conf.trigger_time[0:10]  # 调度日期
        self.gx_date = conf.start_date  # TODO: 供需表取数日期
        self.cities = conf.cities  # TODO
        self.hc = self.create_hive_client()
        self.data = {}
        self.pred_dates_list = list(pd.date_range(conf.start_date, conf.end_date).astype('str'))
        self.pred_dates_str = str(self.pred_dates_list)[1:-1]
        self.budget_limit = conf.budget_limit
        self.city_info_df = conf.city_info_df
        self.city_limit_df = conf.city_limit_df
        self._api_url = conf.api_url
        self.gmv_dt = conf.rely_info.gmv_dt
        self.cr_dt = conf.rely_info.cr_dt
        self.city_group_list = conf.city_group_info
        self.CONF = conf

    def create_hive_client(self):
        app_name = f'Budget_allocation_for_{self.cate}'
        conf = SparkConf().setAppName(app_name)
        spark_context = SparkContext(conf=conf)
        spark = SparkSession.builder \
            .config(conf=conf) \
            .enableHiveSupport() \
            .getOrCreate()
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

        return spark

    def get_data_by_name_and_sql(self, name, sql, cache=True):
        """通过缓存或 hive 获取数据，转换成 DataFrame"""
        if name in self.data and self.data[name].shape[0] != MIN_NUM_OF_DATA:
            return self.data[name]
        start = time.time()
        df = self.hc.sql(sql).toPandas()
        print(f'Execute sql: \n{sql}\nTime cost: {time.time() - start:.2f} seconds')
        if cache:
            self.data[name] = df
        return df

    def get_candidates(self, version):
        for try_num in range(14):
            print(self.step_start_date, self.end_date, len(self.cities))
            sql = candidates_sql.format(cur_day=self.cur_day,
                                        gmv_dt=self.gmv_dt,
                                        try_num=try_num,
                                        version=version)
            print(sql)
            df = self.get_data_by_name_and_sql('hufan_candidates', sql)
            if df.shape[0] != MIN_NUM_OF_DATA:
                df_candicates = pd.merge(df,
                                         self.city_info_df,
                                         on=['stat_date', 'city_id'],
                                         how='right')
                if version[:8] == "zhuanche" :
                    df_candicates['delta_gmv'] = df_candicates['zhuanche_gmv'] * df_candicates['dgmv_ratio']
                elif version[:5] == "chuzu":
                    df_candicates['delta_gmv'] = df_candicates['chuzuche_gmv'] * df_candicates['dgmv_ratio']
                else:
                    error_callback('C端t+1-不支持该业务线分配！',self.CONF)
                if df_candicates['delta_gmv'].isna().sum() > 1:
                    error_callback('C端t+1-gmv获取失败！',self.CONF)
                return df_candicates
        error_callback('C端t+1-候选集获取失败！',self.CONF)

    def get_mps_candidates(self):
        sql = mps_candidate_sql.format(activity_date=self.step_start_date,
                                       gmv_dt=self.gmv_dt,
                                    start_date=self.step_start_date,
                                    end_date=self.end_date,
                                    city_list=self.cities)
        df_candidate = self.get_data_by_name_and_sql('hufan_candidates', sql)
        df_candidate['pk_budget'] = df_candidate['pukuai_gmv'] * df_candidate['kc_subsidy_rate']
        df_candidate['th_budget'] = df_candidate['tehui_gmv'] * df_candidate['th_subsidy_rate']

        lack_cis = df_candidate[df_candidate['delta_gmv'].isna()].city_id.unique()
        print("delta_gmv null的城市:", lack_cis)
        print("delta_gmv null的日期:", df_candidate[df_candidate['delta_gmv'].isna()].stat_date.unique())
        #TODO 修改判断规则
        if len(lack_cis) >= 10:
            print("delta_gmv null的数量:", df_candidate['delta_gmv'].isna().sum())
            print(df_candidate[df_candidate['delta_gmv'].isna()].head(5))
            error_callback('C端t+1-候选集获取失败！', self.CONF)
        cols = ['delta_gmv', 'delta_bp', 'delta_cost', 'total_gmv']
        df_candidate[cols] = df_candidate[cols].fillna(0)
        return df_candidate

    def get_pred_delta_gmv(self, version):
        product = cate_to_pred[self.cate]
        sql = pred_delta_gmv_sql.format(estimate_dt=self.cur_day,
                                        cities=self.cities,
                                        product=product,
                                        version=version)
        return self.get_data_by_name_and_sql('pred_dgmv', sql)

    def get_pred_candidates(self, version1, version2):
        for i in range(max_try_num):
            dt = (datetime.strptime(self.cur_day, '%Y-%m-%d') +
                  timedelta(days=-i)).strftime('%Y-%m-%d')
            print(f'getting candidate table on dt={dt}')
            sql = pred_candidate_sql.format(dt=dt,
                                            version1=version1,
                                            version2=version2,
                                            cities=self.cities)
            df = self.get_data_by_name_and_sql('pred_candidate', sql)
            if not df.empty:
                return df
        error_callback('供需预测表为空',self.CONF)

    def get_pred_cr(self):
        max_try_num = 7
        for try_num in range(max_try_num):
            sql = pred_cr_sql.format(cr_dt=self.cr_dt,
                                     try_num=try_num,
                                     start_date=self.step_start_date,
                                     end_date=self.end_date,
                                     city_list=self.cities)
            print("pred cr sql:")
            print(sql)
            df = self.get_data_by_name_and_sql('pred_cr', sql)
            if not df.empty:
                return df
        error_callback('供需预测客观CR表为空', self.CONF)

    def get_zhuan_roi_rsquare(self):
        for try_num in range(7):
            sql = zhuan_rsquare_sql.format(cur_day=self.cur_day, try_num=try_num)
            df = self.get_data_by_name_and_sql('zhuan_rsquare', sql)
            rsquare_df = pd.merge(df, self.city_info_df, on=['stat_date', 'city_id'], how='right')
            if rsquare_df.shape[0] != MIN_NUM_OF_DATA:
                return rsquare_df
        error_callback('C端t+1-候选拟合r square失败！',self.CONF)
    
    def get_nor_zhuan_rate(self):
        '''专车普通单、橙意单拆分逻辑'''
        sql = nor_zhuan_hufan_rate_sql.format(cur_day = self.cur_day)
        df = self.get_data_by_name_and_sql('nor_zhuan_hufan', sql)
        # 所有城市普通单补贴率最多占比0.8
        df['nor_zhuan_ratio'] = df['nor_zhuan_ratio'].apply(lambda x : x if x <= 0.8 else 0.8)
        return df

    def get_pregap_dignosis_info(self):
        # 获取供需诊断数据
        version_list = json.loads(self.CONF['version_list'])
        fixed_threshold_list = [('rule-fixed-cr_' + str(int(round(version['cr']*100,0))))
                                for version in version_list]
        if self.CONF.rely_info.cr_type == 'objective_exp_openapi_pp' and self.CONF.city_group == "mps":
            product_id = '2_obj'
        elif self.CONF.rely_info.cr_type == 'exp_finish_order_pp' and self.CONF.city_group == "mps":
            product_id = '2_exp'
        elif self.CONF.city_group == "zhuanche_normal":
            product_id = '4_zc_obj'
        elif self.CONF.city_group == "chuzuche_normal":
            product_id = '3_czc_obj'
        else:
            error_callback('B端周分配-诊断数据获取分区错误',self.CONF)
        fixed_threshold_str = str(fixed_threshold_list)[1:-1]
        sql = gap_dignosis_sql.format(cr_dt=self.CONF.rely_info.cr_dt,
                                      product_id=product_id,
                                      fixed_threshold_str=fixed_threshold_str,
                                      st_end_times_str=self.pred_dates_str)
        df = self.get_data_by_name_and_sql('gap_dignosis', sql)
        # 首先判断分区是否有数据
        if df.shape[0] == MIN_NUM_OF_DATA:
            error_callback('B端周分配-获取诊断信息失败 !',self.CONF)
        if len(version_list) != len(list(df['strategy_type'].unique())):
            error_callback("B端周分配-获取诊断数cr分区数据不够",self.CONF)

        gap_dignosis = df.merge(self.CONF.city_info_df,
                                on=['stat_date', 'city_id', 'caller'],
                                how='inner')
        # 过滤当前cr已经大于cr bar的数据，不需要进入诊断数据
        gap_dignosis = gap_dignosis.query('cr<cr_bar')
        if gap_dignosis.shape[0] == MIN_NUM_OF_DATA:
            print("None of city will allocate with B ")
            gap_dignosis = pd.DataFrame(columns=[
                'city_id', 'stat_date', 'fence_id', 'caller', 'strategy_type', 'start_time',
                'start_hour', 'end_time', 'end_hour', 'cr', 'cr_bar', 'gmv_ratio',
                'finish_count_ratio', 'call_count_ratio'
            ])
        else:
            gap_dignosis = gap_dignosis[[
                'city_id', 'stat_date', 'fence_id', 'caller', 'strategy_type', 'start_time',
                'start_hour', 'end_time', 'end_hour', 'cr', 'cr_bar', 'gmv_ratio',
                'finish_count_ratio', 'call_count_ratio'
            ]]
            print("供需诊断分布", gap_dignosis.groupby(['stat_date', 'strategy_type']).count())
        return gap_dignosis, fixed_threshold_list

    def label_search_space_udf(self, json_dict):
        '''
        pyspark udf实现，对在Apollo中的补贴率进行筛选
        :param json_acc_dict:
        :return:
        '''
        def label_search_space(city_id, b_rate, caller):
            rate_dict = json.loads(json_dict)
            label = 0
            if str(city_id) in rate_dict.keys():
                if caller == 'accelerate_card':
                    label = 1
                else:
                    for br in rate_dict[str(city_id)]:
                        # b_rate_list  = get_round_b_rate(br)
                        if (b_rate <= round(br + 0.005, 3)
                                and b_rate >= round(br - 0.005, 3)) or b_rate == 0:
                            label = 1
            # 若该城市没有在apollo配置，则用默认city_id 0(战区城市可用)
            else:
                if caller == 'accelerate_card':
                    label = 1
                else:
                    for br in rate_dict[str(0)]:
                        # b_rate_list  = get_round_b_rate(br)
                        if (b_rate <= round(br + 0.005, 3)
                                and b_rate >= round(br - 0.005, 3)) or b_rate == 0:
                            label = 1
            return label
        return F.udf(label_search_space, IntegerType())
    
    def label_search_space_udf_zhuan(self):
        '''
        pyspark udf实现，对专车盘古补贴率做过滤，2.5pp-12.5pp 和运营确认过
        '''
        def label_search_space_zhuan(b_rate):
            label = 0
            if (b_rate <= round(0.125, 3) and b_rate >= round(0.025, 3)) or b_rate == 0:
                label = 1
            return label

        return F.udf(label_search_space_zhuan, IntegerType())

    def label_search_space_udf_chuzu(self):
        '''
        pyspark udf实现，对出租车盘古补贴率做过滤，0pp、3pp-12pp 步长1pp  和运营确认过
        '''
        def label_search_space_chuzu(b_rate):
            label = 0
            if (b_rate in [round(i,2) for i in np.arange(0.03, 0.13, 0.01)] ) or b_rate == 0:
                label = 1
            return label

        return F.udf(label_search_space_chuzu, IntegerType())

    def get_elastic_b_hourly(self):
        """
        1、获取B端弹性和供需数据结合数据
        2、弹性数据一定会有，允许选择之前分区
        3、如果兜底仍然没有，需要报错人工介入
        """
        accelerate_card_rate_dict, pangu_rate_dict = get_b_rate_dict()
        json_pangu_rate_dict = json.dumps(pangu_rate_dict)
        json_accelerate_card_rate_dict = json.dumps(accelerate_card_rate_dict)
        city_list = [int(city_id) for city_id in self.cities.split(',')]
        stat_date_list = list(self.CONF['city_info_df']['stat_date'].unique())
        stat_hour_list = [int(i) for i in np.linspace(6, 23, 18)]
        b_rate_list = [round(i, 3) for i in np.linspace(0.0, 0.15, 151)]
        multi_index = pd.MultiIndex.from_product(
            [city_list, stat_date_list, stat_hour_list, b_rate_list],
            names=['city_id', 'start_dt', 'start_time', 'b_rate'])
        base_ela_df = pd.DataFrame(index=multi_index).reset_index()
        base_ela_df['estimate'] = base_ela_df['b_rate'] * 1.2  # 弹性
        print(base_ela_df)

        base_ela_py_df = self.hc.createDataFrame(base_ela_df)
        base_ela_py_df.registerTempTable('fake_b_ela_table')
        if self.CONF['city_group'] == "zhuanche_normal":
            pangu_sql = pred_and_pangu_elastic_sql.format(gmv_dt=self.CONF['rely_info']['gmv_dt'],
                                                          st_end_times_str=self.pred_dates_str,
                                                          product_name = '专车')
            
            pangu_df = self.hc.sql(pangu_sql)
            pangu_df = pangu_df.withColumn('label',self.label_search_space_udf_zhuan() \
                                        (pangu_df.b_rate).cast('int')).filter("label == 1").drop("label")
            

        elif self.CONF['city_group'] == "mps":
            pangu_sql = pred_and_pangu_elastic_sql.format(gmv_dt=self.CONF['rely_info']['gmv_dt'],
                                                          st_end_times_str=self.pred_dates_str,
                                                          product_name = '泛快含优含拼（快车（普通快车、A+、车大、巡游网约车）、市内拼车、优享、D1、特惠快车、涅槃）')
            pangu_df = self.hc.sql(pangu_sql)
            pangu_df = pangu_df.withColumn('label',self.label_search_space_udf(json_pangu_rate_dict) \
                                        (pangu_df.city_id, pangu_df.b_rate, pangu_df.caller).cast('int')).filter("label == 1").drop("label")
        
        elif self.CONF['city_group'] == "chuzuche_normal":
            pangu_sql = pred_and_pangu_elastic_sql.format(gmv_dt=self.CONF['rely_info']['gmv_dt'],
                                                          st_end_times_str=self.pred_dates_str,
                                                          product_name = '出租车')
            pangu_df = self.hc.sql(pangu_sql)
            pangu_df = pangu_df.withColumn('label',self.label_search_space_udf_chuzu() \
                                        (pangu_df.b_rate).cast('int')).filter("label == 1").drop("label")

        print("SQL of pred_and_pangu_elastic_sql:", pangu_sql)

        if self.CONF['city_group'] == "zhuanche_normal":
            acc_sql = pred_and_acc_elastic_sql.format(gmv_dt=self.CONF['rely_info']['gmv_dt'],
                                                      st_end_times_str=self.pred_dates_str,
                                                      product_name = '专车')
            acc_df = self.hc.sql(acc_sql)                            
        elif self.CONF['city_group']  == "mps":
            acc_sql = pred_and_acc_elastic_sql.format(gmv_dt=self.CONF['rely_info']['gmv_dt'],
                                                      st_end_times_str=self.pred_dates_str,
                                                      product_name = '泛快含优含拼（快车（普通快车、A+、车大、巡游网约车）、市内拼车、优享、D1、特惠快车、涅槃）')
            acc_df = self.hc.sql(acc_sql)
            acc_df = acc_df.withColumn('label',self.label_search_space_udf(json_accelerate_card_rate_dict) \
                                            ( acc_df.city_id, acc_df.b_rate, acc_df.caller).cast('int')).filter("label == 1").drop("label")
        
        elif self.CONF['city_group'] == "chuzuche_normal":
            acc_sql = pred_and_acc_elastic_sql.format(gmv_dt=self.CONF['rely_info']['gmv_dt'],
                                                      st_end_times_str=self.pred_dates_str,
                                                      product_name = '出租车')
            acc_df = self.hc.sql(acc_sql)
        print("SQL of pred_and_acc_elastic_sql:", acc_sql)
     
        pangu_elastic = pangu_df.toPandas()
        print("盘古弹性天分布：", pangu_elastic.groupby('stat_date').count())
        acc_elastic = acc_df.toPandas()
        print("加速卡弹性天分布：", acc_elastic.groupby('stat_date').count())
        if pangu_elastic.shape[0] == MIN_NUM_OF_DATA or acc_elastic.shape[0] == MIN_NUM_OF_DATA:
            error_callback('B端周分配-获取b端弹性和半小时供需预测数据失败 !',self.CONF)
        return pangu_elastic, acc_elastic

    def get_forecast_data_for_b_fillup(self):
        # 获取天级别供需预测数据
        if self.CONF.rely_info.cr_type == 'objective_exp_openapi_pp' and self.CONF.city_group == "mps":
            sql = forecast_daily_for_b_fillup_sql.format(gmv_dt=self.CONF.rely_info.gmv_dt,
                                                         st_end_times_str=self.pred_dates_str)
        elif self.CONF.rely_info.cr_type == 'exp_finish_order_pp' and self.CONF.city_group == "mps":
            sql = exp_forecast_daily_for_b_fillup_sql.format(
                gmv_dt=self.CONF.rely_info.gmv_dt,
                cr_dt=self.CONF.rely_info.cr_dt,
                st_end_times_str=self.pred_dates_str,
                version="exp",
                product_name='泛快含优含拼（快车（普通快车、A+、车大、巡游网约车）、市内拼车、优享、D1、特惠快车、涅槃）',
                product_id="110100_110400_110800_110103")
        elif self.CONF.city_group == "zhuanche_normal":
            sql = exp_forecast_daily_for_b_fillup_sql.format(gmv_dt=self.CONF.rely_info.gmv_dt,
                                                             cr_dt=self.CONF.rely_info.cr_dt,
                                                             st_end_times_str=self.pred_dates_str,
                                                             product_name='专车',
                                                             version="obj",
                                                             product_id="110500")
        elif self.CONF.city_group == "chuzuche_normal":
            sql = exp_forecast_daily_for_b_fillup_sql.format(gmv_dt=self.CONF.rely_info.gmv_dt,
                                                             cr_dt=self.CONF.rely_info.cr_dt,
                                                             st_end_times_str=self.pred_dates_str,
                                                             product_name='出租车',
                                                             version="obj",
                                                             product_id="120000")
        else:
            error_callback('B端周分配-供需预测天粒度量指标数据获取分区错误',self.CONF)
        df = self.get_data_by_name_and_sql('forecast_daily_data', sql)
        # 首先判断分区是否有数据
        if df.shape[0] == MIN_NUM_OF_DATA:
            error_callback('B端周分配-供需预测天粒度量指标获取失败',self.CONF)

        df_sample = self.CONF.city_info_df.copy()[['city_id', 'stat_date']]
        df_sample = df_sample.drop_duplicates(subset=['city_id', 'stat_date'], keep='first')
        forecast_data = pd.merge(df, df_sample, on=['stat_date', 'city_id'],
                                 how='right').reset_index().astype({
                                     'call_order_cnt': 'float',
                                     'total_gmv': 'float',
                                     'total_finish_order_cnt': 'float'
                                 })
        # 防止分母为0
        forecast_data['call_order_cnt'] = forecast_data['call_order_cnt'].fillna(1)
        # 防止接近0的完单造成计算cr时大于1
        forecast_data['call_order_cnt'] = forecast_data['call_order_cnt'] + 1
        forecast_data['total_gmv'] = forecast_data['total_gmv'].fillna(1)
        # 有非常小的值，小数点四舍五入之后会变为0，整体加0.1
        forecast_data['total_gmv'] = forecast_data['total_gmv'] + 0.1
        if forecast_data.shape[0] != MIN_NUM_OF_DATA:
            return forecast_data
        else:
            error_callback('B端周分配-入参对应的城市、天 供需预测指标获取失败!',self.CONF)

    def get_asp_df(self):
        #获取城市asp数据，过去一周的asp均值作为城市asp
        if self.CONF.city_group == "mps":
            sql = asp_sql.format(cur_day=self.cur_day, product_id="110100_110400_110800_110103")
        elif self.CONF.city_group == "zhuanche_normal":
            sql = asp_sql.format(cur_day=self.cur_day, product_id="110500")
        elif self.CONF.city_group == "chuzuche_normal":
            sql = asp_sql.format(cur_day=self.cur_day, product_id="120000")
        else:
            error_callback('B端周分配-asp量指标数据获取分区错误',self.CONF)
        df = self.get_data_by_name_and_sql("asp_df", sql)
        if (df.shape[0] == MIN_NUM_OF_DATA):
            error_callback('B端周分配-获取asp数据失败!',self.CONF)
        return df


    def get_wyc_ecr(self):
        sql = wyc_ecr_sql.format(start_date=self.step_start_date,
                                 end_date=self.end_date,
                                 city_list=self.cities)
        print("wyc_ecr_sql:")
        print(sql)
        df = self.get_data_by_name_and_sql('wyc_ecr', sql)

        day1 = (datetime.strptime(self.step_start_date, "%Y-%m-%d") - timedelta(days=60)).strftime("%Y-%m-%d")
        print("开始时间:", day1)
        past_day_sql = wyc_ecr_sql.format(start_date=day1, end_date=self.end_date, city_list=self.cities)
        print(past_day_sql)
        pastdf = self.get_data_by_name_and_sql('past_wyc_ecr', past_day_sql)

        df_li = []
        for day in pd.date_range(self.step_start_date, self.end_date).astype(str):
            tt = df.query("stat_date=='%s'" % day)
            city_list = list(map(int, self.cities.split(',')))
            lack_cis = list(set(city_list) - set(tt.city_id.unique()))
            print(day, "当日缺失城市：", lack_cis)
            filldf = pastdf.query("city_id in @lack_cis").groupby('city_id')[['wyc_ecr']].agg('mean').reset_index()
            filldf['stat_date'] = day
            df_li.append(filldf)

        filldf = pd.concat(df_li, ignore_index=True)
        print("填充行数:", len(filldf))
        ecrdf = pd.concat([df, filldf], ignore_index=True)[['city_id', 'stat_date', 'wyc_ecr']]

        if not ecrdf.empty:
            return ecrdf
        else:
            error_callback('上周同期网约车ecr表为空', self.CONF)