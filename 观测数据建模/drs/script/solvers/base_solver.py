import time
# from loguru import logger

from ortools.linear_solver import pywraplp
from   const import Valid_Col
import pandas as pd


class BaseSolver:
    """简化版预算分配求解器"""

    def __init__(self, conf, candidates, is_bp=True, solver_id='CBC', city_must_cost=True, mode='all', total_budget=0, other_constraints=[]):
        self.solver = pywraplp.Solver.CreateSolver(solver_id)
        if mode == 'all':
            self.budget = conf.budget_limit
        elif mode == 'city_bucket':
            self.budget = total_budget
        self.other_constraints = other_constraints
        self.city_group = conf.city_group
        self.is_bp = is_bp
        self.df = candidates
        self.df = candidates[candidates[Valid_Col] == 1]
        self.city_must_spend = city_must_cost
        self.xs = None

    def create_variables(self):
        self.xs = [self.solver.IntVar(0, 1, f'combo_{i}') for i in self.df.index]

    def add_constraints(self):
        """添加求解约束
        目前只有两种约束：预算约束、分城市给补
        """
        cost = self.solver.Sum(self.xs[i] * self.df.loc[i, 'delta_cost'] for i in self.df.index)
        self.solver.Add(cost <= self.budget)
        self.df.groupby(['city_id', 'stat_date']).apply(self.add_city_constraint)
        '''自定义约束'''
        for cons in self.other_constraints:
            print("自定义约束：", cons)
            if cons == 'pk_budget_upper_limit':
                self.add_pk_constarint()

    def add_pk_constarint(self, ratio=0.025):
        day_num = len(self.df.stat_date.unique())
        day_pk_budget = float(self.budget * ratio / day_num)
        print("分框天数:", day_num, "day pukuai budget:", day_pk_budget)
        self.df.groupby(['stat_date']).apply(self.add_day_pk_constraint, day_pk_budget=day_pk_budget)

    def add_day_pk_constraint(self, group_df, day_pk_budget):
        pk_cost = self.solver.Sum(self.xs[i] * self.df.loc[i, 'pk_budget'] for i in group_df.index)
        self.solver.Add(pk_cost >= day_pk_budget)

    def set_objective(self):
        dgmv = self.solver.Sum(self.xs[i] * self.df.loc[i, 'ate'] for i in self.df.index)
        self.solver.Maximize(dgmv)

    def cal_ate(self, lambda_value):
        if self.is_bp:
            self.df['ate'] = self.df['delta_gmv'] + lambda_value * self.df['delta_bp']
        else:
            self.df['ate'] = self.df['delta_gmv']

    def solve(self):
        # 确保 index 为 0 到 length - 1
        lambda_value = self.get_lambda([2, 1.5, 1, 0.5, 0.4, 0.3, 0.25, 0.2, 0.15, 0.1, 0.05, 0.01])
        self.cal_ate(lambda_value)
        self.df = self.df.reset_index()
        self.create_variables()
        self.add_constraints()
        self.set_objective()
        start = time.time()
        status = self.solver.Solve()
        print(f'Solve status: {status}. Time cost: {time.time() - start:.2f}s')
        out_put = self.return_candicate(status)
        return out_put

    def return_candicate(self, status):
        if status == 0:
            index_list = []
            for index in range(len(self.xs)):
                if self.xs[index].solution_value() > 0:
                    index_list.append(index)
            out_put = self.df.loc[index_list, ]
            print(out_put.delta_cost.sum())
            return out_put
        else:
            return pd.DataFrame()


    def add_city_constraint(self, group_df):
        expr = self.solver.Sum(self.xs[i] for i in group_df.index)
        if self.city_must_spend:
            self.solver.Add(expr == 1)
        else:
            self.solver.Add(expr <= 1)


    def get_lambda(self, lambda_list):
        """
        基于预算包选择合适的lambda，筛选出每个城市符合条件的c补组合对应的delta_ate最大的行对应的cost累计，来看最大cost
        """
        if not self.is_bp:
            return 0
        for l in lambda_list:
            total_cost = 0
            self.df['ate'] = self.df['delta_gmv'] + l * self.df['delta_bp']
            for city_id in self.df['city_id'].unique():
                tmp = self.df[self.df['city_id'] == city_id]
                tmp_sort = tmp.sort_values(by='ate', ascending=False).reset_index()
                if tmp_sort.loc[0, 'ate'] > 0:
                    cost = tmp_sort.loc[0, 'delta_cost']
                    total_cost += cost
            if total_cost >= self.budget * 1.1:
                return l
        return l
