#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
"""
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import logging
import traceback
import requests
from pyspark import SparkConf, SparkContext
from pyspark import SparkFiles
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import time
import datetime
import pyspark.sql.functions as pyf
import json
from utils import load_conf

logger = logging.getLogger(__name__)




class Strategy(object):
    def __init__(self, config, stg_param):
        self._config = config
        self._stg_param = stg_param
        self._df = None


    def compute(self):
        stg_param = self._stg_param
        conf = SparkConf().setAppName('bc_budget_allcation_c')
        spark_context = SparkContext(conf=conf)
        hive_context = HiveContext(spark_context)
        #temp = hive_context.sql("select * from bigdata_driver_ecosys_test.mixed_explore_05_fast_only where concat(year, month, day) = '20200507'")

        #print("count %d"%temp.count())
        spark = hive_context
        trigger_day = ''
        if 'trigger_time' in stg_param:
            computeDay = stg_param['trigger_time'].split(' ')[0]
            trigger_day = computeDay
            computeDay = (datetime.datetime.strptime(str(computeDay.replace('00:00:00','')), "%Y-%m-%d")+datetime.timedelta(days = -1) ).strftime("%Y-%m-%d")
        else:
            today = time.strftime("%Y-%m-%d",time.localtime(time.time()))
            trigger_day = (datetime.datetime.strptime(str(today.replace('00:00:00','')), "%Y-%m-%d")+datetime.timedelta(days = -1) ).strftime("%Y-%m-%d")
            computeDay = (datetime.datetime.strptime(str(today.replace('00:00:00','')), "%Y-%m-%d")+datetime.timedelta(days = -2) ).strftime("%Y-%m-%d")

        trace_id = stg_param['trace_id']
        order_id = int(stg_param['order_id'])

        statEnd = computeDay
        roi_low_bound = 0.8
        jizhanbi_default = 0.38
        statStart = (datetime.datetime.strptime(str(computeDay.replace('00:00:00','')), "%Y-%m-%d")+datetime.timedelta(days = -15) ).strftime("%Y-%m-%d")
        predStart = stg_param['start_date']
        predEnd = stg_param['end_date']
        if computeDay >= predStart:
            computeDay = (datetime.datetime.strptime(str(predStart.replace('00:00:00','')), "%Y-%m-%d")+datetime.timedelta(days = -1) ).strftime("%Y-%m-%d")

        removeDays = stg_param['remove_date']
        city_list = stg_param['city_list']
        '''
        if len(sys.argv) > 5:
            predStart = sys.argv[4]
            predEnd = sys.argv[5]
        else:
            predStart = (datetime.datetime.strptime(str(computeDay.replace('00:00:00','')), "%Y-%m-%d")+datetime.timedelta(days = 1) ).strftime("%Y-%m-%d")
            predEnd = (datetime.datetime.strptime(str(computeDay.replace('00:00:00','')), "%Y-%m-%d")+datetime.timedelta(days = 7) ).strftime("%Y-%m-%d")


        '''

        params = {"statStart":statStart,"statEnd": statEnd, "predStart": predStart, "predEnd" : predEnd, "roi_low":roi_low_bound, "computeDay":computeDay}
        tsql = """
        select stat_info.*, pred_info.pred_date, pred_info.total_gmv, pred_info.online_time, delta_info.treat_subsidy_hufan_rate,
        delta_info.pred_dgmv_ratio, delta_info.roi from
        (select  
        city_id
        ,(sum(charge_time_length)/3600)/(sum(online_dur)/60) as charge_time_rate
                ,sum(finish_order_cnt)/7.0 as total_finish_order
                ,cast(sum(gmv) as double)/7.0 as finish_gmv
                ,(sum(charge_time_length)/3600) / sum(finish_order_cnt) as charge_time_per_order
                ,cast((sum(gmv)) / (sum(finish_order_cnt)) as double )as asp
                ,sum(online_dur)/(60 * 7.0) as stat_total_online_time
        from express_data.dm_city_driver_accum_d
        where   concat_ws('-',year,month,day) between '{statStart}' and '{statEnd}' and d_product in (1,2,3)
        and stat_date = concat_ws('-', year, month, day)
        group by 
        city_id
        )stat_info
        inner join
        (
          --select pred_date, city_id, online_time, call_order_cnt as kg_send_cnt, total_gmv from 
          --  prod_smt_dw.supply_demand_prediction
         select d.pred_date, d.city_id, 
     sum(d.total_no_call_order_cnt) as kg_send_cnt,
     sum(d.online_time) as online_time,
     sum(d.predgmv) as total_gmv
     from
     (
      select city_id, stat_date as pred_date,
      case when metric = 'total_no_call_order_cnt' then value  else 0.0 end as total_no_call_order_cnt,
      case when metric = 'online_time' then value  else 0.0 end as online_time,
      case when metric = 'total_gmv' then value  else 0.0 end as predgmv
      from riemann.daily_predict 
      where concat_ws('-',year,month,day) = '{computeDay}' 
      and model_date = '{computeDay}'
      and product = 'kuaijie'
      and stat_date between '{predStart}' and '{predEnd}'
       ) d
      group by d.pred_date, d.city_id    
        ) pred_info
        on stat_info.city_id = pred_info.city_id
        
        left join
        (
        select tdate, city_id, treat_subsidy_hufan_rate,
        cast(get_json_object(prediction, '$.pred_dgmv_ratio') as double) as pred_dgmv_ratio,
        cast(get_json_object(prediction, '$.pred_dgmv_ratio') as double) / treat_subsidy_hufan_rate as roi
        --from bigdata_driver_ecosys_test.tmp_hufan_delta_pred_v4 where dt = '{predStart}'
        from bigdata_driver_ecosys_test.tmp_hufan_delta_pred_v3 where dt = '{computeDay}'
        )delta_info
        on pred_info.city_id = delta_info.city_id
        and pred_info.pred_date = delta_info.tdate
        where (delta_info.roi >= {roi_low} or delta_info.roi is null )
        """.format(**params)
        print(tsql)
        temp = spark.sql(tsql)

        #temporg = temp
        import pandas as pd
        jizhanbi = pd.read_csv('conf/cityjizhanbi.csv',delimiter = ',', header = 'infer', index_col=False)



        mySchema = StructType([ StructField("city_id", IntegerType(), True) \
                                  ,StructField("target", DoubleType(), True)])

        jizhanbidf = spark.createDataFrame(jizhanbi,schema = mySchema)


        hiveContext = hive_context

        cond = [(temp.city_id == jizhanbidf.city_id) ]
        temp = temp.join(jizhanbidf, cond, how = 'left').select(temp["*"],'target')
        temp.na.fill(value=jizhanbi_default,subset=["target"]).show()


        invaliddf = temp.filter("charge_time_rate < target")
        def cal_dptsh(predtotaltsh, charge_tsh_porder, target_charge_time_rate, charge_time_rate, predgmv, asp ):
            #succ_rate = float(np.exp(alpha * gongxu + intercept)/(1 + np.exp(alpha * gongxu + intercept)))
            #return succ_rate
            total_dtsh = predtotaltsh * (target_charge_time_rate - charge_time_rate)
            if charge_tsh_porder is not None and predgmv is not None:
                if charge_tsh_porder != 0 and predgmv != 0 :
                    delta_order = total_dtsh / charge_tsh_porder
                    dgmv_ratio = (delta_order * asp) / predgmv
                    return dgmv_ratio
                else:
                    return 1.0
            else:
                return 1.0


        udf_cal_target_gmv_ratio = udf(cal_dptsh, DoubleType())

        def cal_djizhanbi(predtotaltsh, charge_tsh_porder, dgmv_ratio, charge_time_rate, predgmv, asp ):
            #succ_rate = float(np.exp(alpha * gongxu + intercept)/(1 + np.exp(alpha * gongxu + intercept)))
            #return succ_rate
            if predtotaltsh is not None and asp is not None:
                delta_order = (predgmv * dgmv_ratio)/asp
                #delta_order = dsend_ratio *
                total_dtsh = delta_order * charge_tsh_porder
                djizhanbi = total_dtsh / predtotaltsh
                djizhanbi = djizhanbi + charge_time_rate
                return djizhanbi
            else:
                return 0.0 + charge_time_rate

        udf_cal_pred_new_jizhanbi = udf(cal_djizhanbi, DoubleType())
        invaliddf = invaliddf.withColumn('target_gmv_ratio', udf_cal_target_gmv_ratio(col('online_time'), \
                                                                                      col('charge_time_per_order'), col('target'), col('charge_time_rate'), \
                                                                                      col('total_gmv') , col('asp')))

        invaliddf = invaliddf.withColumn('pred_new_jizhanbi', udf_cal_pred_new_jizhanbi(col('online_time'), \
                                                                                        col('charge_time_per_order'), col('pred_dgmv_ratio'), \
                                                                                        col('charge_time_rate'), \
                                                                                        col('total_gmv') , col('asp')))

        solution = invaliddf.filter("pred_dgmv_ratio >= target_gmv_ratio")
        solution.registerTempTable("mytable")
        tsql="""
        select city_id, pred_date, total_gmv, min(treat_subsidy_hufan_rate),
        charge_time_rate, target, min(pred_new_jizhanbi) as pred_new_jizhanbi
        from mytable
        group by city_id, pred_date, total_gmv,charge_time_rate,target
        """
        resdf = spark.sql(tsql)

        respdf = resdf.toPandas()
        respdf.rename(columns={'min(treat_subsidy_hufan_rate)': 'max(treat_subsidy_hufan_rate)'}, inplace=True)

        respdf['budget'] = respdf['total_gmv'].astype('float') * respdf['max(treat_subsidy_hufan_rate)'].astype('float')


        nosolution = invaliddf.filter("pred_dgmv_ratio < target_gmv_ratio ")
        nosolution.registerTempTable("mytable2")
        tsql="""
        select a.* from
        (select city_id, pred_date, concat(city_id, '_', pred_date) as tempkey, charge_time_rate, total_gmv, target, max(pred_new_jizhanbi) as pred_new_jizhanbi, max(treat_subsidy_hufan_rate) from mytable2
         where roi >= {roi_low}
         --and treat_subsidy_hufan_rate <= 0.08 --maximum subsidy rate 
         group by city_id, pred_date, total_gmv,charge_time_rate,target
        )a
        left join
        (
          select distinct city_id, pred_date from mytable
        )b
        on a.city_id = b.city_id
        and a.pred_date = b.pred_date
        where b.city_id is null and b.pred_date is null
        """.format(**params)
        resdf2 = spark.sql(tsql)
        respdf2 = resdf2.toPandas()#.query("city_id not in (4,7, 9,11,13,15,17,26)")#filter war cities


        respdf2['budget'] = respdf2['total_gmv'].astype('float') * respdf2['max(treat_subsidy_hufan_rate)'].astype('float')

        validdf = temp.filter("charge_time_rate >= target")
        vresdf = validdf.toPandas()
        vresdf[['city_id', 'charge_time_rate','total_finish_order','finish_gmv','charge_time_per_order','asp', \
                'stat_total_online_time',	'pred_date','total_gmv','online_time','target']].drop_duplicates().head(10)
        vresdf['pred_new_jizhanbi'] = vresdf['charge_time_rate']
        vresdf['max(treat_subsidy_hufan_rate)'] = 0.0
        vresdf['budget'] = vresdf['total_gmv'].astype('float') * vresdf['max(treat_subsidy_hufan_rate)'].astype('float')

        fincolumns = ['city_id','pred_date','charge_time_rate','total_gmv','target','pred_new_jizhanbi','max(treat_subsidy_hufan_rate)','budget']
        findf = pd.concat([vresdf[fincolumns], respdf2[fincolumns], respdf[fincolumns]]).drop_duplicates()
        #next append other cities given in city_lists
        showcities = list(findf['city_id'].drop_duplicates())
        preddays = list(findf['pred_date'].drop_duplicates())
        noshowcities = []
        for city in city_list:
            if not city in showcities:
                noshowcities.append(city)

        appendlist = []
        if len(noshowcities) > 0:
            #append
            for city in noshowcities:
                for day in preddays:
                    temptpl = (city, day, None, 1e-6, jizhanbi_default, None, 0.0, 0.0)
                    appendlist.append(temptpl)

        appendf = pd.DataFrame(appendlist, columns=fincolumns)

        findf = pd.concat([findf, appendf])
        findf.rename(columns={'max(treat_subsidy_hufan_rate)': 'treat_subsidy_hufan_rate'}, inplace=True)
        findf['dt'] = trigger_day
        findf['trace_id'] = trace_id
        findf['order_id'] = order_id
        findf['city_id'] = findf['city_id'].astype('int')
        #findf.to_csv("./%s_finres.csv"%(computeDay))
        #tohive
        hiveContext.setConf("hive.exec.dynamic.partition", "true")
        hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        #df3 = hiveContext.createDataFrame(findf, schema)

        # Auxiliar functions
        def equivalent_type(f):
            if f == 'datetime64[ns]': return TimestampType()
            elif f == 'int64': return LongType()
            elif f == 'int32': return IntegerType()
            elif f == 'float64': return FloatType()
            else: return StringType()

        def define_structure(string, format_type):
            try: typo = equivalent_type(format_type)
            except: typo = StringType()
            return StructField(string, typo)

        def pandas_to_spark(hive_context , pandas_df):
            columns = list(pandas_df.columns)
            types = list(pandas_df.dtypes)
            struct_list = []
            for column, typo in zip(columns, types):
                struct_list.append(define_structure(column, typo))
            p_schema = StructType(struct_list)
            return hive_context.createDataFrame(pandas_df, p_schema)

        spark_df = pandas_to_spark(hiveContext , findf)
        spark_df.repartition(1).registerTempTable("mytable")
        dbName = 'bigdata_driver_ecosys_test'
        tableName = 'c_budget_allocation_tmp'
        fintable = dbName + "." + tableName

        sql="""
        CREATE TABLE IF NOT EXISTS `bigdata_driver_ecosys_test`.`%s`(`city_id` INT COMMENT ' city_id ', `pred_date` STRING COMMENT ' pred_date ', `charge_time_rate` DECIMAL(19,6) COMMENT ' charge_time_rate ', `total_gmv` DECIMAL(38,8) COMMENT ' xxx for elastic embedding ', `target` DECIMAL(19,6) COMMENT ' target_charge_time_rate', `pred_new_jizhanbi` DECIMAL(19,6) COMMENT ' pred_new_jizhanbi ', `treat_subsidy_hufan_rate` DECIMAL(19,6) COMMENT ' treat_subsidy_hufan_rate', `budget` DECIMAL(38,6) COMMENT ' budget ', `trace_id` STRING COMMENT ' trace_id ',`order_id` BIGINT COMMENT 'order_id')
        PARTITIONED BY (`dt` STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
          'field.delim' = ' ',
          'colelction.delim' = ',',
          'mapkey.delim' = ':',
          'serialization.format' = '    '
        )
        STORED AS
          INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
          OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION 'hdfs://DClusterNmg4/user/bigdata_driver_ecosys_test/luowenjuan/testhiveout/%s'
        """%(tableName, tableName)

        hiveContext.sql(sql)
        #df3.repartition(2).write.mode('overwrite').partitionBy("year", "month", "day","hour").saveAsTable(fintable)
        outputsqlcmd = """insert overwrite table %s PARTITION (dt) select city_id, pred_date ,charge_time_rate , total_gmv ,target,pred_new_jizhanbi ,treat_subsidy_hufan_rate,budget,trace_id,order_id,dt  from mytable"""%(fintable)
        hiveContext.sql(outputsqlcmd)
        spark_context.stop()
        city_list_str = ','.join([str(x) for x in city_list])
        removeDays_str = "','".join([str(x) for x in removeDays])
        #"city_id":1,
        #"predict_rate":10.5,
        #"amount":100000,
        #"amount_type":"headquarters_budget",
        #"caller":"dape-aps",
        #"product_line":"kuaiche"

        resdf = findf.query("city_id in (%s) & pred_date not in ('%s')"%(city_list_str, removeDays_str))
        myres = resdf[['city_id','total_gmv','treat_subsidy_hufan_rate','budget']].groupby(['city_id']).sum().reset_index()
        myres['predict_rate'] = myres['budget'] / myres['total_gmv']
        myres = myres[['city_id','predict_rate','budget']]
        myres.rename(columns = {'budget':'amount'},inplace = True)
        myres['amount_type'] = myres['city_id'].apply(lambda x : 'headquarters_budget')
        myres['caller'] = myres['city_id'].apply(lambda x : 'dape-aps')
        myres['product_line'] = myres['city_id'].apply(lambda x : 'kuaiche')
        myres['amount'] = myres['amount'] * 100.0 #fen
        return myres,order_id


    def get_strategy(self):
        stg_param = self._stg_param
        df = self.compute()
        return df


    def generate(self):

        try:
            #{"city_budget": {"city_budget_list": [{ "city_id ":1, "predict_rate ":0.0, "amount ":0.0, "amount_type ": "c ", "caller ": "dape-aps ", "product_line ": "kuaiche "},{ "city_id ":2, "predict_rate ":0.0, "amount ":0.0, "amount_type ": "c ", "caller ": "dape-aps ", "product_line ": "kuaiche "},{ "city_id ":4, "predict_rate ":0.0, "amount ":0.0, "amount_type ": "c ", "caller ": "dape-aps ", "product_line ": "kuaiche "}]}, "errno": 0, "operator": "admin", "order_id": 22, "errmsg": ""}
            df, order_id = self.get_strategy()
            #dfstr = df.to_json(orient='records')
            reslist = []
            for i in range(0, len(df)):
                tempdict = df.iloc[i].to_dict()
                reslist.append(tempdict)

            resp = {
                "errno": 0 ,
                "errmsg": "",
                "city_budget": json.dumps({"city_budget_list":reslist}),
                "order_id": order_id,
                "operator":"admin"
            }
            #'''

            return resp
        except :
            return {
                "order_id": -1,
                "operator":"admin",
                "city_budget": json.dumps({'city_budget_list' :
                                          [{"city_id":-1,"amount":0.0,"predict_rate":0.0,"caller":"dape-aps","amount_type":"headquarters_budget","product_line ": "kuaiche "}] ,
                                      }) ,
                "errno": 1,
                "errmsg": "C端填坑预算分配失败"
            }

if __name__ == '__main__':
    config = load_conf("./conf/job_config.json")
    stg_param = json.loads(sys.argv[1])
    stg = Strategy(config, stg_param )
    temp_res = stg.generate()
    print(stg_param)
    #for key in stg_param.keys():
    #    print(key, stg_param[key])

    print(temp_res)