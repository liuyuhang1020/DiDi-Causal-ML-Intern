#!/usr/bin/env python
import os
import toml
import logging
import json
import datetime
logger = logging.getLogger(__name__)


import calendar
import datetime
import pandas as pd
import json
import tempfile

def execute_cmd(cmd):
    logger.info(f'executing command: {cmd}')
    ret = os.system(cmd)
    if ret != 0:
        raise Exception(f'Failed to execute command: {cmd}')


def execute_hive_sql(sql, queue):
    _, output = tempfile.mkstemp(prefix='smt_')
    cmd = 'hive --hiveconf mapreduce.job.queuename={queue} -e "{sql}" > {output}'.format(
        queue = queue,
        sql=sql,
        output=output)
    print(cmd)
    os.system(cmd)
    return pd.read_csv(output, sep="\t")


def dic_to_str(dic):
    cands = {}
    for k in dic:
    	cands[str(k)] = dic[k]
    return str(cands)

def get_14_days_before(dt):
    dt_14 = datetime.datetime.strptime(dt, '%Y-%m-%d') - datetime.timedelta(days = 14)
    dt_14 = datetime.datetime.strftime(dt_14, '%Y-%m-%d')
    return dt_14

def get_k_days_before(dt, k):
    dt_k = datetime.datetime.strptime(dt, '%Y-%m-%d') - datetime.timedelta(days = k)
    dt_k = datetime.datetime.strftime(dt_k, '%Y-%m-%d')
    return dt_k


def get_city_hour_tbl():
    res = []
    for city_id in range(1,400):
        for hour in range(24):
            res.append([city_id,hour])
    res = pd.DataFrame(res)
    res.columns = ['city_id', 'hour']
    return res

def date_n_less(t1, t2):
    if datetime.datetime.strptime(t1, '%Y-%m-%d') >= datetime.datetime.strptime(t2, '%Y-%m-%d'):
        return True
    else:
        return False
    

def check_partition(queue, tbl, date):
        sql = """select max(concat_ws('-',year,month,day)) dt from {table} where concat_ws('-',year,month,day) > '2021-03-01' and trace_id = '123456'""".format(table = tbl)
        partition = execute_hive_sql(sql, queue)
        print(partition)
        print(partition['dt'])
        newest_date = partition['dt'][0]
        print(newest_date, date)
        if date_n_less(newest_date, date):
            return True, newest_date
        else:
            return False, newest_date