#!/usr/bin/env python

import os
import logging
import json
import tempfile
import pandas as pd

logger = logging.getLogger(__name__)


def load_conf(conf_file):
    if not os.path.exists(conf_file):
        raise Exception('Failed to find config file: %s'%conf_file)
    with open(conf_file) as fconf:
        config = json.load(fconf)
        logger.info('config: %s'%config)
        return config


def execute_cmd(cmd):
    logger.info('executing command: %s'%cmd)
    ret = os.system(cmd)
    if ret != 0:
        raise Exception('Failed to execute command: %s'%cmd)


def execute_cmd_and_get_result(cmd):
    logger.info('executing command: %s'%cmd)
    with os.popen(cmd) as f:
        content = f.read()
    return content

def execute_hive_sql(sql):
    _, output = tempfile.mkstemp(prefix='smt_')
    cmd = 'hive --hiveconf mapreduce.job.queuename=root.dev_pricing_driver_prod -e "{sql}" > {output}'.format(
        sql=sql,
        output=output
    )
    print(cmd)
    os.system(cmd)
    return pd.read_csv(output, sep="\t")

def hdfs_path_exist(hdfs_path):
    cmd = 'hadoop fs -test -e %s'%hdfs_path
    try:
        execute_cmd(cmd)
    except:
        return False
    return True


def time_to_float_hour(time_str):
    parts = time_str.split(':')
    if len(parts) < 2:
        return -1
    ret = 2*float(parts[0])
    if float(parts[1]) >= 15 and float(parts[1]) <= 45:
        ret += 1
    elif float(parts[1]) > 45:
        ret += 2
    return ret