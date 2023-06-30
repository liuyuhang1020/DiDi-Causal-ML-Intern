"""在线策略系统"""
from   const import Valid_Col
import pandas as pd
from   policy.base_policy import BasePolicy
from   callback import error_callback
import itertools
import numpy as np
from data.hive_sql import *
from solvers.base_solver import *


class MPSFilterPolicy(BasePolicy):
    """mps解空间过滤策略"""

    def run(self):
        pred_candidate = self.df
        ecrdf = self.dao.get_wyc_ecr()
        crdf = self.dao.get_pred_cr()
        '''读取城市分框信息'''
        filter_pred_candidate_list = []
        print("城市分框数量：", len(self.dao.city_group_list))
        if len(self.dao.city_group_list) > 0:
            for group_info in self.dao.city_group_list:
                sdate, edate = group_info['start_date'], group_info['end_date']
                if type(group_info['city_list']) == str:
                    cis = list(map(int, group_info['city_list'].split(',')))
                elif type(group_info['city_list']) == list:
                    cis = group_info['city_list']
                city_bucket_budget = group_info['budget_upperbound']
                print("城市分框：", "开始日期：", sdate, "结束日期：", edate, "预算：", city_bucket_budget)
                print("城市分框 cis:", cis)

                '''生成每个ecr分框的预算'''
                group_ecrdf = ecrdf.query("city_id in @cis and stat_date >= '%s' and stat_date <= '%s'" % (sdate, edate))
                bin_costdf = self.gen_ecr_bucket_budget(group_ecrdf, sdate, edate, cis, city_bucket_budget)
                if len(bin_costdf) == 0:
                    continue
                else:
                    group_crdf = crdf.query("city_id in @cis and stat_date >= '%s' and stat_date <= '%s'" % (sdate, edate))
                    '''生成ecr-cr分框的锚点补贴率'''
                    cr_ratedf = self.gen_cr_bucket_rate(group_crdf, group_ecrdf, bin_costdf)
                    '''过滤候选集'''
                    cr_ratedf['min_combo_rate'] = cr_ratedf['avg_rate'].apply(lambda x: x * 0.8).astype(float)
                    cr_ratedf['max_combo_rate'] = cr_ratedf['avg_rate'].apply(lambda x: x * 1.2).astype(float)
                    # 临时兜底约束
                    cr_ratedf['max_combo_rate'] = cr_ratedf.apply(lambda x: 0.01 if x.city_id in (8, 25) else x.max_combo_rate, axis=1)
                    cr_ratedf['min_combo_rate'] = cr_ratedf.apply(lambda x: 0.0 if x.city_id in (8, 25) else x.min_combo_rate, axis=1)
                    cr_ratedf['max_combo_rate'] = cr_ratedf.apply(lambda x: 0.02 if x.city_id in (14, 16, 22) else x.max_combo_rate, axis=1)
                    cr_ratedf['min_combo_rate'] = cr_ratedf.apply(lambda x: 0.0 if x.city_id in (14, 16, 22) else x.min_combo_rate, axis=1)

                    group_candidate = cr_ratedf.merge(pred_candidate, on=['city_id', 'stat_date'], how='left')
                    print("城市分框，候选集数量：", len(group_candidate))
                    filter_group_candidate = group_candidate.query("is_candidate==1 and crate>=min_combo_rate and crate<=max_combo_rate")
                    print("城市分框，过滤后的候选集数量：", len(filter_group_candidate))
                    filter_pred_candidate_list.append(filter_group_candidate)
        filter_pred_candidate = pd.concat(filter_pred_candidate_list) # 过滤后的候选集
        print('过滤后的候选集数量:', len(filter_pred_candidate))
        print(filter_pred_candidate.head(5))
        return filter_pred_candidate


    def gen_ecr_bucket_budget(self, ecrdf, start_date, end_date, city_list, city_bucket_budget):
        '''生成ecr分框预算'''
        ecrdf['ecr_bin'] = ecrdf['wyc_ecr'].apply(self.gen_ecr_bucket)
        ecrdf = ecrdf[~ecrdf['ecr_bin'].isna()]

        dates = list(pd.date_range(start_date, end_date).astype(str))
        fkhf_list = list(itertools.product(city_list, dates, np.arange(0, 0.11, 0.0005)))
        ratedf = pd.DataFrame(fkhf_list, columns=['city_id', 'stat_date', 'fkhf'])
        print("ratedf:", ratedf.head(5))
        print("citys:", city_list)

        df = pd.merge(ecrdf, ratedf, on=['city_id', 'stat_date'])
        df['ecr_dgmv_ratio'] = df.apply(self.gen_ecr_dgmv_ratio, axis=1)
        gmvdf = self.df[['city_id', 'stat_date', 'total_gmv']].drop_duplicates() # candidate表
        opti_data = pd.merge(df, gmvdf, on=['city_id', 'stat_date'], how='left')

        opti_data['delta_gmv'] = opti_data['ecr_dgmv_ratio'] * opti_data['total_gmv']
        opti_data['delta_cost'] = opti_data['fkhf'] * opti_data['total_gmv']
        opti_data[Valid_Col] = 1

        print("ecr bucket opti_data number:", len(opti_data))
        if len(opti_data) == 0:
            return pd.DataFrame([], columns=['ecr_bin', 'budget'])
        else:
            # 整数规划求解
            solver = BaseSolver(self.CONF, opti_data, is_bp=False, city_must_cost=True, mode='city_bucket', total_budget=city_bucket_budget)
            resdf = solver.solve()
            bin_costdf = resdf.groupby(['ecr_bin'])[['delta_cost']].agg(sum).reset_index().rename(columns={'delta_cost': 'budget'})
            print("ecr分框的预算:")
            print(bin_costdf)
            return bin_costdf


    def gen_ecr_bucket(self, x):
        if x <= 0.59:  # x > 0.15
            return 0
        elif x > 0.59 and x <= 0.635:
            return 1
        elif x > 0.635 and x <= 0.66:
            return 2
        elif x > 0.66 and x <= 0.685:
            return 3
        elif x > 0.685: # and x <= 0.78
            return 4


    def gen_ecr_dgmv_ratio(self,x):
        w = 0.65
        if x.ecr_bin == 0:
            return 0.744 * (x.fkhf ** (w))
        elif x.ecr_bin == 1:
            return 0.69 * (x.fkhf ** (w))
        elif x.ecr_bin == 2:
            return 0.64 * (x.fkhf ** (w))
        elif x.ecr_bin == 3:
            return 0.597 * (x.fkhf ** (w))
        elif x.ecr_bin == 4:
            return 0.542 * (x.fkhf ** (w))



    def gen_cr_bucket(self,x):
        if x > 0 and x <= 0.5:
            return 0
        elif x > 0.5 and x <= 0.6:
            return 1
        elif x > 0.6 and x <= 0.68:
            return 2
        elif x > 0.68 and x <= 0.72:
            return 3
        elif x > 0.72 and x <= 0.76:
            return 4
        elif x > 0.76 and x <= 0.8:
            return 5
        elif x > 0.8 and x <= 0.85:
            return 6
        elif x > 0.85:
            return 7


    def gen_cr_bucket_rate(self, score, ecrdf, bin_costdf):
        cr_col = 'score'
        score = score[score[cr_col] > 0]
        score['bin'] = score[cr_col].apply(self.gen_cr_bucket)
        score['bin_val'] = score['bin'].apply(lambda x: (x + 1) / 8 if x <= 3 else (0.2 * (x + 1) if x <= 5 else 0.25 * x))

        gmvdf = self.df[['city_id', 'stat_date', 'total_gmv']].drop_duplicates()  # candidate表
        weightdf = pd.merge(score, gmvdf, on=['city_id', 'stat_date'])
        weightdf['weight'] = weightdf.apply(lambda x: x['total_gmv'] * x['bin_val'], axis=1)

        df_li = []
        ecr_bins = ecrdf.ecr_bin.unique()
        print("ecr bins:", ecr_bins)
        for ecr_bin in ecr_bins:
            tmpdf = ecrdf.query("ecr_bin==%d" % ecr_bin)[['city_id', 'stat_date']]
            tmpdf2 = pd.merge(weightdf, tmpdf)
            if len(tmpdf2) > 0:
                bin_budget = bin_costdf.query("ecr_bin==%d" % ecr_bin)['budget'].iloc[0] # ecr分框预算
                avg_rate = (bin_budget) / (tmpdf2['weight'].sum())  # 泛快呼返
                print("avg_rate:", avg_rate)
                tmpdf2['avg_rate'] = tmpdf2['bin_val'].apply(lambda x: avg_rate * x)
                tmpdf2['ecr_bin'] = ecr_bin
                df_li.append(tmpdf2)
            else:
                continue
        cr_ratedf = pd.concat(df_li, ignore_index=True)
        return cr_ratedf




