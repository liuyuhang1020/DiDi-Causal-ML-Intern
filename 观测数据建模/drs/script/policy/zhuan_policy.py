"""在线策略系统"""
from   const import Valid_Col
import pandas as pd
from   policy.base_policy import BasePolicy
from   callback import error_callback

class ZCFilterPolicy(BasePolicy):
    """专车解空间过滤策略"""

    def run(self):
        columns_list = self.df.columns
        df = self.df
        df = df.sort_values(['city_id', 'stat_date', 'combo_idx'])
        df = self.cal_cur_cost(df)
        greedy_ans = self.budget_alloction_by_roi(df)
        candidate = self.find_best_subsidy_rate(greedy_ans)
        greedy_candicate_range = self.gen_subsidy_range(candidate)
        new_df = pd.merge(self.df, greedy_candicate_range, on=['city_id', 'stat_date'], how='left')
        # TODO: parallelize
        new_df[Valid_Col] = new_df.apply(self.filter_row, axis=1)
        return new_df[columns_list]

    @staticmethod
    def filter_row(row):
        """根据roi分配结果给的补贴率解空间范围过滤候选集"""
        if row[Valid_Col] == 0:
            return 0
        if row.zhuan_hufan_rate > row.hufan_rate_upper or row.zhuan_hufan_rate < row.hufan_rate_lower:
            return 0
        return 1

    @staticmethod
    def cal_cur_cost(df):
        """计算dcost，多加一列"""
        city_id_init = df.loc[0, "city_id"]
        date_init = df.loc[0, "stat_date"]
        cal_budget = 0
        for index, cor in df.iterrows():
            if cor['city_id'] == city_id_init and cor['stat_date'] == date_init:
                cur_budget = cor['delta_cost'] - cal_budget
                cal_budget = cor['delta_cost']
            else:
                cur_budget = cor['delta_cost']
                cal_budget = 0
            city_id_init = cor['city_id']
            date_init = cor['stat_date']
            df.loc[index, 'cur_budget'] = cur_budget
        return df

    def budget_alloction_by_roi(self, df):
        df = pd.merge(df, self.dao.city_limit_df, on=['city_id', 'stat_date'], how='left')
        df['limit_min'] = df['limit_min'].fillna(0)
        df['limit_max'] = df['limit_max'].fillna(1)

        doudi_df = df.query('zhuan_hufan_rate <= limit_min')
        rest_df = df.query('zhuan_hufan_rate > limit_min and zhuan_hufan_rate <= limit_max')
        rest_df = rest_df.sort_values('roi_bp', ascending=False)
        rest_money = doudi_df['cur_budget'].sum()
        print('兜底至少需要',rest_money)
        if rest_money > self.dao.budget_limit:
            error_callback("c端t+1分配- 预算不足以分配兜底补贴",self.CONF)
        cal = 0
        for index, cor in rest_df.iterrows():
            cal += cor['cur_budget']
            rest_df.loc[index, 'cal_budget'] = cal
        valid_df = rest_df.query('cal_budget <= %d' % (self.dao.budget_limit - rest_money))
        greedy_ans = pd.concat([valid_df, doudi_df]).groupby(['city_id', 'stat_date']).agg({
            "cur_budget":
            "sum"
        }).reset_index().rename(columns={"cur_budget": "temp_budget"})
        print('greedy_ans',greedy_ans)
        return greedy_ans

    def find_best_subsidy_rate(self, greedy_ans):
        df = pd.merge(self.df, greedy_ans, on=['city_id', 'stat_date'], how='left')
        df = df.query('delta_cost <= temp_budget')
        print('df',df)
        df_key = df.groupby(['city_id', 'stat_date']).agg({'combo_idx': 'max'}).reset_index()
        print('df_key',df_key)
        greedy_candicate = pd.merge(df_key,
                                    self.df,
                                    on=['city_id', 'stat_date', 'combo_idx'],
                                    how='left')
        print(greedy_candicate)
        return greedy_candicate

    def gen_subsidy_range(self, greedy_candicate):
        r_square_df = self.dao.get_zhuan_roi_rsquare()
        greedy_candicate = pd.merge(greedy_candicate,
                                    r_square_df,
                                    on=['city_id', 'stat_date'],
                                    how='left')
        greedy_candicate = pd.merge(greedy_candicate,
                                    self.dao.city_limit_df,
                                    on=['city_id', 'stat_date'],
                                    how='left')
        greedy_candicate['limit_min'] = greedy_candicate['limit_min'].fillna(0)
        greedy_candicate['limit_max'] = greedy_candicate['limit_max'].fillna(1)
        greedy_candicate[['hufan_rate_upper',
                          'hufan_rate_lower']] = greedy_candicate.apply(self.cal_subsidy_range,
                                                                        axis=1,
                                                                        result_type='expand')

        return greedy_candicate[['city_id', 'stat_date', 'hufan_rate_upper', 'hufan_rate_lower']]

    def cal_subsidy_range(self, x):
        if x.r2_roi_cb > 0.7:
            upper, lower = x.zhuan_hufan_rate * 1.2, x.zhuan_hufan_rate * 0.8
        elif x.r2_roi_cb > 0.5:
            upper, lower = x.zhuan_hufan_rate * 1.3, x.zhuan_hufan_rate * 0.7
        else:
            upper, lower = x.zhuan_hufan_rate * 1.4, x.zhuan_hufan_rate * 0.6
        return min(x.limit_max, upper), max(x.limit_min, lower)