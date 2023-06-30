#!/usr/bin/env python

import os
import re
import hashlib
import logging
import pandas as pd
import findspark
findspark.init()
from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)
        
# def singleton(cls):
#     _instance = {}

#     def _singleton(*args, **kargs):
#         if cls not in _instance:
#             _instance[cls] = cls(*args, **kargs)
#         return _instance[cls]

#     return _singleton


# @singleton
class SparkClient(object):
    def __init__(self, conf, app_name):
        self._conf = conf
        self._app_name = app_name
        self._session = None
        self._use_cache = int(self._conf['spark'].get('use_cache', 0))
        if self._use_cache:
            self._cache_path = '/tmp/spark_client_cache'
            if not os.path.exists(self._cache_path):
                os.makedirs(self._cache_path)
    
    def _replace_param(self, s, key, val):
        if type(key) != type("") or type(val) != type(""):
            return s
        ps = r"\$\{" + key + "\}"
        ptn = re.compile(ps)
        return ptn.sub("{}".format(val), s)

    def exec_sql(self, sql, param_dict):
        if self._session is None:
            self._session = SparkSession.builder \
                .appName(self._app_name) \
                .master("yarn") \
                .config("spark.yarn.queue", self._conf['spark']['job_queue']) \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .config("mapred.input.dir.recursive", "TRUE") \
                .config("hive.mapred.supports.subdirectories", "TRUE") \
                .enableHiveSupport() \
                .getOrCreate()
        for key, val in param_dict.items():
            sql = self._replace_param(sql, key, val)
        print(sql)
        logger.info(f'executing sql:\n {sql}')
        return self._session.sql(sql).toPandas()
    
    def _load_sql(self, sql_file):
        with open(sql_file) as fsql:
            return fsql.read()

    def exec_sql_file(self, sql_file, param_dict):
        if not self._use_cache:
            sql = self._load_sql(sql_file)
            return self.exec_sql(sql, param_dict)
        param_keys = sorted(param_dict.keys())
        suffix = '_'.join([f'{key}-{param_dict[key]}' for key in param_keys])
        md = hashlib.md5()
        md.update(suffix.encode('utf-8'))
        suffix_md5 = md.hexdigest()
        cache_file = os.path.join(self._cache_path, f'{os.path.basename(sql_file)}_{suffix_md5}')
        print(f"cache_file 路径: {cache_file}")
        if os.path.exists(cache_file):
            return pd.read_csv(cache_file, sep='\t')
        sql = self._load_sql(sql_file)
        res_df = self.exec_sql(sql, param_dict)
        res_df.to_csv(cache_file, sep='\t', index=None)
        return res_df
