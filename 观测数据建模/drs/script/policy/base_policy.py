"""在线策略系统"""
from   const import Valid_Col


def run_policies(candidates, policy_list, dao, conf,mode='serial'):
    """在候选集上执行指定的策略

    :param candidates: 候选集 dataframe
    :param policy_list: 策略列表
    :param dao: data access object
    :param mode: 多个策略的执行模式，serial 串行，否则并行
    :return: 候选集 dataframe
    """
    if mode == 'serial':
        for policy in policy_list:
            p = policy(candidates, dao, conf)
            candidates = p.run()
        return candidates
    else:
        raise NotImplementedError(f'Unknown model: {mode}')


class BasePolicy:

    def __init__(self, candidates, dao, conf):
        self.df = candidates
        self.dao = dao
        self.CONF = conf

    def run(self):
        return self.df


class WCDiffPolicy(BasePolicy):
    """泛快、网出不同向策略"""

    def run(self):
        df = self.df
        cr_df = self.dao.get_pred_cr()
        new_df = df.merge(cr_df, on='city_id', how='left')
        # TODO: parallelize
        df[Valid_Col] = new_df.map(self.filter_row, axis=1)
        return df

    @staticmethod
    def filter_row(row):
        """根据 cr 和价差过滤可能会导致泛快正网出负的候选集"""
        if row[Valid_Col] == 0:
            return 0
        if row.cr > 0.85:
            if row.kh < 0.05 or row.hp > 0.1:
                return 0
        elif 0.7 < row.cr <= 0.85:
            if row.kh < 0.05:
                return 0
        return 1
