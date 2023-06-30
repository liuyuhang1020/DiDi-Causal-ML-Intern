import os
from pandas import describe_option
import requests
import tempfile
import logging
import pandas as pd

from datetime import datetime
from common.utils import execute_cmd

logger = logging.getLogger(__name__)

class HiveClient(object):

    def __init__(self, conf):
        self._conf = conf
        self._queue = conf['spark']['job_queue']

    def execute_hive_sql(self, sql):
        _, output = tempfile.mkstemp(prefix='smt_')
        cmd = 'hive --hiveconf mapreduce.job.queuename={queue}  -e "{sql}" > {output}'.format(
            queue=self._queue,
            sql=sql,
            output=output
        )
        execute_cmd(cmd)
        return pd.read_csv(output, sep="\t")

    def upload_to_hive(self, df, columns, dt, table):
        _, filename = tempfile.mkstemp(prefix='smt_ts')
        print("dumps to file {}. table {}".format(filename, table))
        dt = datetime.strptime(dt, "%Y-%m-%d")
        df[columns].to_csv(filename, sep='\t', index=None, header=None)
        cmd = '''hive --hiveconf mapreduce.job.queuename={queue} -e "
        load data local inpath '{filename}' overwrite into table {table} partition(year='{year:>04d}',month='{month:>02d}',day='{day:>02d}');
        "'''.format(
            queue=self._queue,
            filename=filename,
            table=table,
            year=dt.year, month=dt.month, day=dt.day
        )
        execute_cmd(cmd)
        if self._queue == "root.dev_pricing_driver_prod":
            tag = '''hadoop fs -touchz /user/pricing_driver/smt_stg/hive/smt_stg/{table}/year='{year:>04d}'/month='{month:>02d}'/day='{day:>02d}'/_SUCCESS'''.format(table=table.split('.')[1],year = dt.year,month=dt.month,day=dt.day)
        else:
            tag = '''hadoop fs -touchz /user/prod_pricing_driver/prod_smt_stg/hdp/{table}/year='{year:>04d}'/month='{month:>02d}'/day='{day:>02d}'/_SUCCESS'''.format(table=table.split('.')[1],year = dt.year,month=dt.month,day=dt.day)
        execute_cmd(tag)
        resp = requests.get("""http://100.69.92.35:8021/stg/data_tag?partition={dt}&table={table}"""\
            .format(dt=datetime.strftime(dt, '%Y-%m-%d'),table=table))
        logger.info('add tag {table}({dt}) returns {resp}'.format(table=table, dt=dt, resp=resp.text))

    def upload_to_partition(self, df, columns, dt, sub_partition, table):
        _, filename = tempfile.mkstemp(prefix='smt_ts')
        print("dumps to file {}. table {}".format(filename, table))
        dt = datetime.strptime(dt, "%Y-%m-%d")
        df[columns].to_csv(filename, sep='\t', index=None, header=None)
        cmd = '''hive --hiveconf mapreduce.job.queuename={queue} -e "load data local inpath '{filename}' overwrite into table {table} partition(year='{year:>04d}',month='{month:>02d}',day='{day:>02d}',{sub_partition});"'''.format(
            queue=self._queue,
            filename=filename,
            table=table,
            year=dt.year, month=dt.month, day=dt.day,
            sub_partition=sub_partition
        )
        execute_cmd(cmd)
        if self._queue == "root.dev_pricing_driver_prod":
            tag = '''hadoop fs -touchz /user/pricing_driver/smt_stg/hive/smt_stg/{table}/year='{year:>04d}'/month='{month:>02d}'/day='{day:>02d}'/{sub_partition}/_SUCCESS'''.format(table=table.split('.')[1],year = dt.year,month=dt.month,day=dt.day,sub_partition=sub_partition)
        else:
            tag = '''hadoop fs -touchz /user/prod_pricing_driver/prod_smt_stg/hdp/{table}/year='{year:>04d}'/month='{month:>02d}'/day='{day:>02d}'/{sub_partition}/_SUCCESS'''.format(table=table.split('.')[1],year = dt.year,month=dt.month,day=dt.day,sub_partition=sub_partition)
        execute_cmd(tag)

    def copy_partiton(self, table, src_dt, dest_dt, depth=0):
        if self._queue == "root.dev_pricing_driver_prod":
            base_path = '/user/pricing_driver/smt_stg/hive/smt_stg/{table}'.format(table=table.split('.')[-1])
        else:
            base_path = '/user/prod_pricing_driver/prod_smt_stg/hdp/{table}'.format(table=table.split('.')[-1])
        src_dt = datetime.strptime(src_dt, '%Y-%m-%d')
        dest_dt = datetime.strptime(dest_dt, '%Y-%m-%d')
        src_path = '{base_path}/year={year:>04d}/month={month:>02d}/day={day:>02d}'\
            .format(base_path=base_path, year=src_dt.year, month=src_dt.month, day=src_dt.day)
        dest_path = '{base_path}/year={year:>04d}/month={month:>02d}/day={day:>02d}'\
            .format(base_path=base_path, year=dest_dt.year, month=dest_dt.month, day=dest_dt.day)

        if depth == 0:
            cmd = '''hadoop fs -mkdir -p {dest_path}'''.format(dest_path=dest_path)
            execute_cmd(cmd)
            cmd = '''hadoop fs -cp {src_path}/smt_* {dest_path}'''.format(src_path=src_path, dest_path=dest_path)
            execute_cmd(cmd)

            partition="year='{year:>04d}',month='{month:>02d}',day='{day:>02d}'"\
                .format(year=dest_dt.year, month=dest_dt.month, day=dest_dt.day)
            cmd = '''hive --hiveconf mapreduce.job.queuename={queue} -e "
            alter table {table} add partition({partition})
            "'''.format(queue=self._queue, table=table, partition=partition)
            execute_cmd(cmd)

            cmd = '''hadoop fs -touchz {dest_path}/_SUCCESS'''.format(dest_path=dest_path)
            execute_cmd(cmd)
        else:
            glob_symbols = '/'.join(['*'] * (depth - 1))
            list_path_cmd = """hadoop fs -ls {src_path}/{glob_symbols} |awk '{{print $8}}'"""\
                .format(src_path=src_path, glob_symbols=glob_symbols)
            with os.popen(list_path_cmd) as fpath:
                src_path_len = len(src_path.rstrip('/'))
                src_sub_paths = fpath.read().split('\n')
                for src_sub_path in src_sub_paths:
                    if src_sub_path.strip('\n') == '':
                        continue
                    suffix = src_sub_path[src_path_len:]
                    dest_sub_path = '{dest_path}/{suffix}'.format(dest_path=dest_path, suffix=suffix)
                    cmd = '''hadoop fs -mkdir -p {dest_sub_path}'''.format(dest_sub_path=dest_sub_path)
                    execute_cmd(cmd)
                    cmd = '''hadoop fs -cp {src_sub_path}/smt_* {dest_sub_path}'''\
                        .format(src_sub_path=src_sub_path, dest_sub_path=dest_sub_path)
                    execute_cmd(cmd)
                    date_partition="year='{year:>04d}',month='{month:>02d}',day='{day:>02d}'"\
                        .format(year=dest_dt.year, month=dest_dt.month, day=dest_dt.day)
                    suffix_segs = suffix.split('/')
                    sub_partition = ''.join(
                        [f",{seg.split('=')[0]}='{seg.split('=')[1]}'" for seg in suffix_segs]
                    )
                    partition = date_partition + sub_partition
                    cmd = '''hive --hiveconf mapreduce.job.queuename={queue} -e "
                    alter table {table} add partition({partition})
                    "'''.format(queue=self._queue, table=table, partition=partition)
                    execute_cmd(cmd)
                    cmd = '''hadoop fs -touchz {dest_sub_path}/_SUCCESS'''.format(dest_sub_path=dest_sub_path)
                    execute_cmd(cmd)
        resp = requests.get("""http://10.74.113.54:8021/stg/data_tag?partition={dt}&table={table}"""\
            .format(dt=datetime.strftime(dest_dt, '%Y-%m-%d'),table=table))
        logger.info('add tag {table}({dt}) returns {resp}'.format(table=table, dt=dest_dt, resp=resp.text))
    
    def partition_ready(self, table, dt_str, depth=0):
        dt = datetime.strptime(dt_str, '%Y-%m-%d')
        if self._queue == "root.dev_pricing_driver_prod":
            table_path = '/user/pricing_driver/smt_stg/hive/smt_stg/{table}/year={year:>04d}/month={month:>02d}/day={day:>02d}'\
                .format(table=table.split('.')[-1], year = dt.year, month=dt.month, day=dt.day)
        else:
            table_path = '/user/prod_pricing_driver/prod_smt_stg/hdp/{table}/year={year:>04d}/month={month:>02d}/day={day:>02d}'\
                .format(table=table.split('.')[-1], year = dt.year, month=dt.month, day=dt.day)
        if depth == 0:
            try:
                cmd = 'hadoop fs -test -e {table_path}/_SUCCESS'.format(table_path=table_path)
                execute_cmd(cmd)
                return True
            except:
                return False
        else:
            glob_symbols = '/'.join(['*'] * (depth - 1))
            list_path_cmd = """hadoop fs -ls {table_path}/{glob_symbols} |awk '{{print $8}}'"""\
                .format(table_path=table_path, glob_symbols=glob_symbols)
            with os.popen(list_path_cmd) as fpath:
                sub_paths = fpath.read().split('\n')
            try:
                for sub_path in sub_paths:
                    if sub_path.strip() == '':
                        continue
                    cmd = 'hadoop fs -test -e {sub_path}/_SUCCESS'.format(sub_path=sub_path)
                    execute_cmd(cmd)
                return True
            except:
                return False
