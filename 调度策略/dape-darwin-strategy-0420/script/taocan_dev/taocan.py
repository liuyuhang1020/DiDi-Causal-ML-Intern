import numpy as np
import pandas as pd
import sys
import os
import logging
import requests
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark import SparkFiles
from pyspark.sql import HiveContext
# from pyspark.sql.functions import col, udf, date_sub, explode, split, date_add
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark import *
from pyspark.sql import *
from sql_file import *
import json
import datetime
money_unit = 100
logger = logging.getLogger(__name__)
def load_conf(conf_file):
    if not os.path.exists(conf_file):
        raise Exception('Failed to find config file: %s' % conf_file)
    with open(conf_file) as fconf:
        config = json.load(fconf)
        logger.info('config: %s' % config)
        return config


class combo_strategy(object):
    def __init__(self, config, param):
        self._param = param 
        self._config = config
        conf = SparkConf().setAppName('combo_1')
        self.sc = SparkContext(conf=conf)
        self.hc = HiveContext(self.sc)

    def get_info(self):
        cur_date = self._param['cur_day']
        city_list = self._param['city_list']
        total_budget = self._param['budget']/money_unit
        start_date = self._param['start_date']
        real_end_date = self._param['end_date']
        black_list = self._param['blackdict']
        return cur_date, city_list, total_budget, start_date, real_end_date, black_list
    
    def data_prepare(self):
        cur_date, city_list, total_budget, start_date, real_end_date, black_dict = self.get_info()
        # cur_date = (pd.Timestamp(cur_date) + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
        print('当前时间：', cur_date)
        self.param = {
            'cur_day' : cur_date,
            'total_budget' : total_budget,
            'start_date' : start_date,
            'real_end_date' : real_end_date,
            'black_dict' : black_dict
        }
        #套餐中不考虑战区城市/选城的时候就已经保证剔除
        real_days = list(pd.date_range(start_date, real_end_date))
        self.param['real_days'] = real_days
        print('real_days',real_days)
        city_list = [str(i) for i in city_list]
        city_list_1 = ",".join(city_list)
        # start = pd.Timestamp(start_date)
        # end_date_7 = (start+datetime.timedelta(days=6)).strftime("%Y-%m-%d")
        # self.param['end_date_7'] = end_date_7
        pred_dates = list(pd.date_range(start_date, real_end_date).astype('str'))
        print(pred_dates)
        self.time_range = pred_dates
        self.param['pred_dates'] = "','".join(pred_dates)
        self.param['city_list'] = city_list_1
        #在提取数据时候防止当前日期表没有数据生成，给5天的往前搜索区间
        for i in range(5):
            self.param['pred_try_num'] = i
            data_gmv = self.hc.sql(taocan_df.format(**self.param))
            if len(data_gmv.head(1)) != 0:
                break
        GMV_data = data_gmv.toPandas()
        print('total_GMV', GMV_data.value.sum())
        print('大盘GMV数据',GMV_data)

        for i in range(5):
            self.param['pred_bound_num'] = i
            data_bound = self.hc.sql(taocan_up_low.format(**self.param))
            if len(data_bound.head(1)) != 0:
                break
        up_data = data_bound.toPandas()
        print('边界数据:\n', up_data)

        for i in range(5):
            self.param['pred_ela_num'] = i
            data_rate = self.hc.sql(taocan_rate.format(**self.param))
            if len(data_bound.head(1)) != 0:
                break
        elacity_data = data_rate.toPandas()
        # import pdb;pdb.set_trace()
        # 预算不足七天的时候进行缩放
        up_data.minimal_net_subsidy = up_data.minimal_net_subsidy.apply(lambda x: x*(len(real_days)/7))
        print('弹性数据:\n', elacity_data.head(5))

        city_list = list(up_data.city_id)
        #将弹性，补贴率和分天的补贴率和在一起
        data_gmv_ela = pd.merge(GMV_data, elacity_data, on='city_id', how='inner')
        data_gmv_ela['subsidy'] = data_gmv_ela['value'] * data_gmv_ela['b_rate']
        print('最终数据集：',data_gmv_ela)
        #求出在给定的补贴率选择情况下，看看兜底的情况下补贴率是什么情况，最高限制为0.003（因为平均的值是0.002左右）然后为了保证预算分配不至于很容易超预算，向下取整
        df_new = pd.DataFrame()
        self.df_qianyi = pd.DataFrame()
        #只选择有上下限的城市进行分配
        for city in city_list:
            flag = 0
            low_bound = float(up_data[up_data['city_id'] == str(city)].minimal_net_subsidy)
            for rate in [0.001, 0.002, 0.003, 0.004]:
                df_set = data_gmv_ela.query('city_id == %s & b_rate == %f' % (str(city), rate))
                week_budget = df_set.subsidy.sum()
                if  week_budget > low_bound:
                    higher = data_gmv_ela.query('city_id == %s & b_rate == %f' %(str(city), rate+0.001))['subsidy'].sum()
                    if abs(higher - week_budget) > abs(week_budget - low_bound):
                        week_budget = higher
                        rate += 0.001
                        flag = 1
                    else:
                        break
                est_elesity = df_set.estimate.mean()
                mean_gmv = df_set.value.mean()
                temp = pd.DataFrame({
                    'city_id' : [city],
                    'pred_week' : [self.param['start_date']],
                    'b_rate' : [rate],
                    'estimate' : [est_elesity],
                    'budget' : [week_budget],
                    'GMV' : [mean_gmv]
                })
                #保底值在千分位的四舍五入
                if rate == 0.001 or (flag == 1 and rate == 0.002):
                    self.df_qianyi = self.df_qianyi.append(temp)
                if flag == 1:
                    break
                df_new = df_new.append(temp)
        print('qianyi:', self.df_qianyi)
        self.data4 = data_gmv_ela
        return df_new, up_data

    def linearresult(self):
        from ortools.linear_solver import pywraplp
        df, up_data = self.data_prepare()
        print('now data', df)
        #把选择的城市的下限额度先给出，保存在DICT里面
        doudi_strategy = {}
        for city in self.param['city_list'].split(','):
            doudi_strategy[int(city)] = up_data[up_data['city_id'] == city].minimal_net_subsidy.sum()
        df['city_id'] = df['city_id'].astype(int)
        nums = [i for i in range(len(df))]
        df.insert(0, 'idx', nums)
        x = {}
        #为了尽量能保证约束的严禁，这边选择整数规划作为第一选择
        solver = pywraplp.Solver.CreateSolver('CBC')
        solver.max_time_in_seconds = 240.0
        print(len(df))
        for i in range(len(df)):
            x[i] = solver.IntVar(0.0, 1, 'x[%i]' % (i))
        print(len(x))

        remain_budget = self.param['total_budget'] - sum(list(doudi_strategy.values()))
        print('保底已经使用：\n', sum(list(doudi_strategy.values())))
        print('remain_budget:', remain_budget)
        if remain_budget < 0:
            raise Exception("exceed lower bound!!!!!")
        solver.Add(solver.Sum([x[i] * df['budget'].iloc[i] for i in range(0, len(x))]) <= remain_budget)     
        solver.Add(solver.Sum([x[i] * df['budget'].iloc[i] for i in range(0, len(x))]) >= 0)
        #以一个城市一周为力度，对于多个补贴值进行添加权重，得到的就是剩下的预算的重新分配情况下的每个城市的预算
        for city in list(set(df['city_id'])):
            # print('chengshi:', city)
            # print(type(city))
            citydiff = df.query("city_id == %d & pred_week == '%s'" % (city, self.param['start_date']))
            idxlist = []
            for j in range(0, len(citydiff)):
                idxlist.append(citydiff.iloc[j]['idx'])
                    # print(idxlist)
            solver.Add(solver.Sum([x[idxlist[i]] for i in range(0, len(idxlist))]) == 1)
            # solver.Add(0.01 <= solver.Sum([x[idxlist[i]] for i in range(0, len(idxlist))]))
        print('Number of variables =', solver.NumVariables())
        print('Number of constraints =', solver.NumConstraints())

        solver.Maximize(solver.Sum([x[i] * df['estimate'].iloc[i] * df['GMV'].iloc[i] for i in range(0, len(x))])) #目标使一段时间的GMV增量最大化
        status = solver.Solve()  #求和所有的GMV增加量，然后最大化GMV求解

        reslist = []
        #如果有解
        if status == pywraplp.Solver.OPTIMAL:
            print('Solution:')
            print('Objective value =', solver.Objective().Value())  #?什么是OBJECTIVE VALUE
            tempdict = {}
            for i in range(0, len(x)):
                cur_val = x[i].solution_value() #这个值还有小于0的吗？？？
                # print('tempkey_all',tempkey_all)
                if cur_val > 0.0:
                    # print ('cur_val',cur_val)
                    tempkey = '%d_%s' % (df['city_id'].iloc[i], df['pred_week'].iloc[i]) 
                    if not tempkey in tempdict:  #index=（城市，日期）, value=（当前数值，数据堆的序列号）
                        tempdict[tempkey] = (cur_val,i)
                    else:    
                        his_max_val,his_idx = tempdict[tempkey] #对统一日期保留最大的数值
                        if cur_val > his_max_val:
                            tempdict[tempkey] =  (cur_val,i)
            for (k,v) in tempdict.items():
                reslist.append(v[1])    #为什么是V[1], v不应该是日期吗就只有一维啊
            choose_scheme = df.iloc[reslist, :]
        else:
            #兜底如果没有解，就按照先弹性后城市的顺序进行分配
            print('no optimal result')
            city_seen, picked = set(), list()
            total_cost = 0
            df_backup = self.df_qianyi
            # print(df_backup)
            nums = [i for i in range(len(df_backup))]
            df_backup.insert(0, 'idx', nums)
            df_backup['dgmv'] = df_backup['estimate'] * df_backup['GMV']
            df_backup = df_backup.sort_values(by='dgmv', ascending=False)
            for _, row in df_backup.iterrows():
                if row['city_id'] not in city_seen:
                    cost = row['budget']
                    city_seen.add(row['city_id'])
                    total_cost += cost
                    if total_cost > remain_budget:
                        break
                    picked.append(row['idx'])
            choose_scheme = df_backup[df_backup['idx'].map(lambda x: x in picked)]
            print(choose_scheme)
            print(len(picked))
            choose_scheme = choose_scheme.sort_values(by='city_id', ascending=False)
        print('realoc_answer:\n', choose_scheme.head(10))
        print('规划总补贴额：\n', choose_scheme.budget.sum())

        #将重新分配的加上兜底的就是最终分配的结果
        for city in list(choose_scheme.city_id):
            temp_df = choose_scheme[choose_scheme['city_id'] == city]
            if not temp_df.empty:
                temp_val = float(temp_df['budget'])
                doudi_strategy[int(city)] += temp_val
        print('现在的兜底：\n', doudi_strategy)
        budget_after_realoc = sum(doudi_strategy.values())
        if budget_after_realoc < self.param['total_budget']:
            diff = self.param['total_budget'] - budget_after_realoc
            for city in list(doudi_strategy.keys()):
                ratio = doudi_strategy[city]/budget_after_realoc
                doudi_strategy[city] += ratio*diff
        now_total = sum(doudi_strategy.values())
        diff = self.param['total_budget']-now_total
        
        fin_df = pd.DataFrame()
        for city in self._param['city_list']:
            doudi_val = doudi_strategy[city] + diff
            diff = 0
            rate = 0.003
            if city in list(choose_scheme.city_id):
                # realoc_val = float(choose_scheme[choose_scheme['city_id'] == city].budget)
                rate = float(choose_scheme[choose_scheme['city_id'] == city].b_rate)
            mean_gmv = self.data4.query('city_id == %d & b_rate == %f'%(city, rate))['value'].sum()
            est_elesity = self.data4.query('city_id == %d & b_rate == %f'%(city, rate))['estimate'].mean()
            realoc_fin = doudi_val
            temp = pd.DataFrame({
                    'city_id' : [city],
                    'pred_week' : [self.param['start_date']],
                    'b_rate' : [rate],
                    'estimate' : [est_elesity],
                    'budget' : [realoc_fin],
                    'GMV' : [mean_gmv]
                })
            fin_df = fin_df.append(temp)
        # fin_df.to_csv('dim_week_result.csv')
        choose_scheme = fin_df
        print('final_anwser\n', choose_scheme)
        print('分配最终结果：\n', choose_scheme.budget.sum())
        return choose_scheme

    def block_strategy(self, df):
        from collections import defaultdict
        from copy import deepcopy
        all_city_block = self._param['remove_date']
        block_dict = defaultdict(list)
        for city in self._param['city_list']:
            block_dict[city] = deepcopy(all_city_block)
        # print(block_dict)
        single_city_date = self.param['black_dict']
        for i in range(len(single_city_date)):
            blocked = single_city_date[i]['black_list'][0]
            date_b = single_city_date[i]['date']
            if date_b in all_city_block:
                continue
            for num in blocked['city']:
                block_dict[num].append(date_b)
        print('all date block\n', block_dict)
        res = pd.DataFrame()
        # remove_lenth = len(self._param["remove_date"])
        #将一周的预算额度，在屏蔽之后分到每天，并重建一个新的dataframe
        for city in self._param['city_list']:
            city_diff = df.query('city_id == %d' % (city))
            blocked_city = block_dict[city]
            for idx, date in enumerate(blocked_city):
                if date not in self.param['pred_dates']:
                    blocked_city.remove(date)
            real_realoc_days = len(self.param['real_days'])-len(blocked_city)
            print('实际周长：%d, 屏蔽时长：%d'%(len(self.param['real_days']), len(blocked_city)))
            # print('屏蔽城市列表:\n', blocked_city)
            day_budget = city_diff['budget'].sum()/real_realoc_days
            gmv = city_diff['GMV'].sum()/real_realoc_days
            estimate = city_diff['estimate'].sum()
            rate = city_diff['b_rate'].mean()
            for day in pd.date_range(self.param['start_date'], self.param['real_end_date']).astype(str):
                if day in blocked_city:
                    continue
                temp = pd.DataFrame({
                    'city_id' : [city],
                    'pred_date' : [day],
                    'b_rate': [rate],
                    'budget' : [day_budget],
                    'gmv' : [gmv],
                    'elacity' : [estimate]
                }) 
                res = res.append(temp)
        # res.to_csv('final_res.csv')
        return res
        
    def extract_json_format(self, df):
        reslist= []
        temp_df = df.copy()
        temp_df = temp_df.astype({
            'city_id' : 'int',
            'b_rate' : 'float',
            'budget' : 'float'
        })
        total_taocan_money = 0
        st_end_time_set = set(self.time_range)
        # for city_id in  list(set(temp_df.city_id)):
        for city_id in self._param['city_list']:
            day_taocan_subsidy = {}
            city_df = temp_df.query('city_id == {}'.format(city_id))
            t_total_gmv = city_df['gmv'].sum()*money_unit
            taocan_pred_rate = city_df['b_rate'].sum()
            taocan_pred_subsidy = city_df['budget'].sum()*money_unit #工程接受的是分纬度的金额
            for _,row in city_df.iterrows():
                day_pred_date = row.pred_date
                if row.budget >= 0:
                    day_taocan_subsidy[day_pred_date] = row.budget*money_unit
                else:
                    day_taocan_subsidy[day_pred_date] = 0.0
        
            taocan_json = {
                'city_id' : int(city_id),
                'pred_rate' : taocan_pred_rate,
                'gmv' : t_total_gmv,
                'amount' : taocan_pred_subsidy,
                "amount_type": self._param['amount_type'],
                "caller": "combo",
                "product_line": "kuaiche",
                "rec_detail" : { 
                    "combo_detail":day_taocan_subsidy
                }
            }
            total_taocan_money += taocan_pred_subsidy
            reslist.append(taocan_json)
        print('call back：')
        print('c money: {}'.format(total_taocan_money))
        return reslist

    def gen_output(self):
        df = self.linearresult()
        res = self.block_strategy(df)
        reslist = self.extract_json_format(res)
        order_id = self._param['order_id']
        operator = self._param['operator']
        resp = {
            "errno": 0,
            "errmsg": "",
            "city_budget": json.dumps({"city_budget_list": reslist}),
            "order_id": order_id,
            "operator": operator
        }
        # print(resp)
        return resp

if __name__ == '__main__':
    config = load_conf("./conf/job_config.json")
    stg_param = json.loads(sys.argv[1])
    print(stg_param)


