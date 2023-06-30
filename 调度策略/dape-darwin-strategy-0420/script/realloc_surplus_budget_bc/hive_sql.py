#!/usr/bin/env python
# -*- encoding: utf-8 -*-


# [B端诊断和分时弹性信息]

gap_dignosis_sql = '''
select
    city_id,
    result_date as stat_date,
    fence_id,
    tool,
    start_time,
    cast(substring(start_time,1,2) as int) as start_hour,
    end_time,
    case when substring(end_time,4,2) != '30' then cast(substring(end_time,1,2) as int)-1 else cast(substring(end_time,1,2) as int)  end as end_hour,
    cr,
    cr_bar,
    gmv_ratio,
    finish_count_ratio,
    call_count_ratio
from
    -- prod_smt_dw.smt_budget_diagnosis_info
    prod_smt_dw.smt_budget_diagnosis_info_v2 
    where
    concat_ws('-', year, month, day) = date_sub("{cur_day}", {gap_fillup_try_num}+0)
    and strategy_type='fixed_threshold_72'
    and result_date in ({st_end_times_str})
'''


pred_and_pangu_elastic_sql = '''
select
    elestic_pangu_df.city_id,
    elestic_pangu_df.start_dt as stat_date,
    cast(elestic_pangu_df.start_time as int) as stat_hour,
    elestic_pangu_df.b_rate,
    elestic_pangu_df.estimate as delta_tsh,
    cast(pred_df.total_gmv as float) as total_gmv,
    cast(pred_df.online_time as float) as online_time,
    cast(pred_df.total_finish_order_cnt as float) as total_finish_order_cnt,
    cast(pred_df.call_order_cnt as float) as call_order_cnt,
    "智能盘古" as tool
from 
    (
    -- 2、pangu hourly elastic info
    select
        city_id,
        start_dt,
        start_dt as end_dt,
        start_time,
        start_time as end_time,
        b_rate,
        estimate
    from
        -- gulfstream.sinan_online_predict_public_function_estimate
        fake_b_ela_table
        ) elestic_pangu_df 
    inner join (
    -- pred hourly base info
    select
        city_id,
        estimate_date as stat_date,
        hour,
        sum(case when key_id = 'gmv' then key_value  else 0.0 end) as total_gmv,
        sum(case when key_id = 'online_dur' then key_value  else 0.0 end) as online_time,
        sum(case when key_id = 'finish_order_cnt' then key_value  else 0.0 end) as total_finish_order_cnt,
        coalesce(sum(case when key_id = 'gmv' then key_value  else 0.0 end)/sum(case when key_id = 'finish_order_cnt' then key_value else 0.0 end),10.0) as asp,
        sum(case when key_id = 'compete_call_cnt' then key_value  else 0.0 end) as call_order_cnt
    from
        mp_data.app_trip_mp_sd_hour_forecast_di
    where
        dt = date_sub("{cur_day}",{b_fillup_try_num}+2)
        and estimate_date in ({st_end_times_str})
        and product_name = '网约车'
    group by
        city_id,
        estimate_date,
        hour) pred_df 
on elestic_pangu_df.city_id = pred_df.city_id
and elestic_pangu_df.start_dt = pred_df.stat_date
and cast(elestic_pangu_df.start_time as int) = cast(pred_df.hour as int)
'''

pred_and_acc_elastic_sql = '''
select
        elestic_acc_card_df.city_id,
        elestic_acc_card_df.start_dt as stat_date,
        cast(elestic_acc_card_df.start_time as int) as stat_hour,
        elestic_acc_card_df.b_rate,
        elestic_acc_card_df.estimate as delta_tsh,
        cast(pred_df.total_gmv as float) as total_gmv,
        cast(online_time as float) as online_time,
        cast(pred_df.total_finish_order_cnt as float) as total_finish_order_cnt,
        cast(pred_df.call_order_cnt as float) as call_order_cnt,
        "加速卡" tool
    from
        (
            --3、 acc_card hourly elastic info
            select distinct
                city_id,
                start_dt,
                start_dt as end_dt,
                start_time,
                start_time as end_time,
                cast(0.04 as float ) as b_rate,
                cast(0.048 as float) as estimate
            from
                -- gulfstream.sinan_online_predict_public_function_estimate
                fake_b_ela_table
                ) elestic_acc_card_df
        inner join (
       -- pred hourly base info
            select
                city_id,
                estimate_date as stat_date,
                hour,
                sum(case when key_id = 'gmv' then key_value  else 0.0 end) as total_gmv,
                sum(case when key_id = 'online_dur' then key_value  else 0.0 end) as online_time,
                sum(case when key_id = 'finish_order_cnt' then key_value  else 0.0 end) as total_finish_order_cnt,
                coalesce(sum(case when key_id = 'gmv' then key_value  else 0.0 end)/sum(case when key_id = 'finish_order_cnt' then key_value  else 0.0 end),10.0) as asp,
                sum(case when key_id = 'compete_call_cnt' then key_value else 0.0 end) as call_order_cnt
            from
                mp_data.app_trip_mp_sd_hour_forecast_di
            where
                dt = date_sub("{cur_day}",{b_fillup_try_num}+2)
                and estimate_date in ({st_end_times_str})
                and product_name = '网约车'
            group by
                city_id,
                estimate_date,
                hour) pred_df 
        on elestic_acc_card_df.city_id = pred_df.city_id
        and elestic_acc_card_df.start_dt = pred_df.stat_date
        and cast(elestic_acc_card_df.start_time as int) = cast(pred_df.hour as int)
'''
daily_gmv_sql= '''
select
    a.city_id,
    a.pred_date as stat_date,
     a.pred_date,
    b.total_gmv as gmv_daily
from
    df_sample a
    left join pred_dapan_info_gmv b 
    on a.city_id = b.city_id
        and a.pred_date = b.pred_date
'''

# 【其他模型预测表信息】
#  todo: online_time -> online_time_no_pangu, call_order_cnt -> call_order_cnt_no_hufan
pred_sql = '''
            select a.pred_date,
            a.city_id,
            a.total_gmv as total_gmv,
            a.online_time as online_time,
            a.finish_order_cnt as finish_order_cnt,
            a.asp as asp,
            a.call_order_cnt as call_order_cnt,
            a.strive_order_cnt as strive_order_cnt,
            coalesce(b.total_gmv,a.total_gmv/3, 0.0) as pukuai_gmv
            from
            (
              select 
                    stat_date as pred_date,
                    city_id,
                    sum(if(metric='total_gmv', value, 0)) as total_gmv,
                    sum(if(metric='online_time', value, 0)) as online_time,
                    sum(if(metric='total_finish_order_cnt', value, 0)) as finish_order_cnt,
                    coalesce(sum(if(metric='total_gmv', value, 0))/sum(if(metric='total_finish_order_cnt', value, 0)),10.0) as asp
                    ,sum(if(metric='total_no_call_order_cnt', value, 0)) as call_order_cnt
                    ,sum(if(metric='strive_order_cnt', value, 0)) as strive_order_cnt
                from riemann.daily_predict_w
                -- from riemann.holiday_predict_whk   
                where concat_ws('-',year,month,day) = date_sub('{cur_day}',{pred_try_num}+2)
                and stat_date in ({st_end_times_str})
                and product = 'wyc'
                and city_id in ({city_list})
                group by stat_date,city_id
            ) a
            left join
            (
              select city_id, 
                   stat_date as pred_date,
                   sum(case when metric = 'total_gmv' then value  else 0.0 end) as total_gmv,
                   sum(case when metric = 'online_time' then value  else 0.0 end) as online_time,
                   sum(case when metric = 'total_finish_order_cnt' then value  else 0.0 end) as total_finish_order_cnt,
                   coalesce(sum(case when metric = 'total_gmv' then value  else 0.0 end)/sum(case when metric = 'total_finish_order_cnt' then value  else 0.0 end),10.0) as asp,
                   sum(case when metric = 'total_no_call_order_cnt' then value  else 0.0 end) as call_order_cnt,
                   sum(case when metric = 'strive_order_cnt' then value else 0.0 end) as strive_order_cnt
                 from riemann.daily_predict_kuai
              where concat_ws('-',year,month,day) = date_sub('{cur_day}',{pred_try_num}+2)
              and stat_date in ({st_end_times_str})
              and city_id in ({city_list})
              and product = 'kuai'
              group by city_id,stat_date
            )b
            on a.pred_date=b.pred_date
            and a.city_id=b.city_id
           '''

alpha_sql = '''
            select
                city_id,
                cast(get_json_object(params, '$.type') as string) as type,
                cast(get_json_object(params, '$.k1') as double) as k1,
                cast(get_json_object(params, '$.k2') as double) as k2,
                cast(get_json_object(params, '$.k3') as double) as k3,
                cast(get_json_object(params, '$.alpha') as double) as alpha,
                cast(get_json_object(params, '$.intercept') as double) as intercept,
                cast(get_json_object(params, '$.gongxu_max') as double) as gongxu_max,
                cast(get_json_object(params, '$.gongxu_min') as double) as gongxu_min,
                cast(get_json_object(params, '$.diaodu_b_max') as double) as diaodu_b_max,
                cast(get_json_object(params, '$.diaodu_b_min') as double) as diaodu_b_min
            from bigdata_driver_ecosys_test.diaodu_gongxu_finish_rate_model_params_v2 
            where dt = date_sub('{cur_day}',{alpha_try_num})
            '''
# 【c表信息】
c_ori_budget_sql = ''' select coalesce(sum(budget),0.0) as total_budget                 
                                from bigdata_driver_ecosys_test.c_budget_allocation_tmp
                                where pred_date in ({st_end_times_str})
                                and dt = "{cur_day}"
                                and city_id in ({city_list})
                                -- and dt = '2021-02-26'
                                -- and trace_id = '{trace_id}' '''
c_elasticity_sql = '''
                    select 
                        city_id,
                        tdate as pred_date,
                        cast(round(treat_subsidy_hufan_rate, 3) * 1000 as int) as treat_subsidy_hufan_rate_new,
                        cast(get_json_object(prediction, '$.pred_dsend_ratio') as double) as pred_dsend_ratio
                    -- from bigdata_driver_ecosys_test.tmp_hufan_delta_pred
                    -- from bigdata_driver_ecosys_test.tmp_hufan_delta_pred_dsend_v2
                    -- from bigdata_driver_ecosys_test.tmp_hufan_delta_pred_dsend_v3_causal_inference
                    from bigdata_driver_ecosys_test.tmp_hufan_delta_pred_dsend_v5_causal_inference2
                    where dt = date_sub('{cur_day}',{c_elasticity_try_num})
                '''

c_fillup_sql = '''  select  city_id,
                            pred_date,
                            charge_time_rate,
                            total_gmv,
                            target,
                            pred_new_jizhanbi,
                            treat_subsidy_hufan_rate,
                            budget,
                            trace_id,
                            order_id
                    from bigdata_driver_ecosys_test.c_budget_allocation_tmp
                    where  dt = date_sub('{cur_day}',{c_fillup_try_num} + 2)
               '''

pred_and_c_fillup_sql = '''
                    select
                        ori_tb.city_id
                        ,ori_tb.pred_date
                        --,dayofweek(ori_tb.pred_date) as day_of_week
                        ,pmod(datediff(ori_tb.pred_date, '1920-01-01') - 3, 7) as day_of_week
                        ,coalesce(pred_info.total_gmv,c_fillup_month_tb.total_gmv,0.0) as total_gmv
                        ,coalesce(pred_info.pukuai_gmv,0.0) as pukuai_gmv
                        -- ,coalesce(cast(round(c_fillup_month_tb.budget/pred_info.pukuai_gmv, 3) * 1000 as int),0) as treat_subsidy_hufan_rate
                        , 0.0 as treat_subsidy_hufan_rate -- 去除C端填坑
                        -- ,coalesce(c_fillup_month_tb.budget,0.0) as budget 
                        , 0.0 as budget  -- 去除C端填坑
                        ,coalesce(c_fillup_month_tb.trace_id,'123') as trace_id
                    from
                    (
                        select * 
                        from df_sample
                    ) ori_tb
                    left join
                    (
                        select * 
                        from month_level_fillup_df
                    )c_fillup_month_tb
                    on ori_tb.pred_date = c_fillup_month_tb.pred_date
                    and ori_tb.city_id = c_fillup_month_tb.city_id
                    left join
                    (
                    {pred_sql}
                    )pred_info
                    on ori_tb.pred_date = pred_info.pred_date
                    and ori_tb.city_id = pred_info.city_id    
                '''
# 【b 表信息】
b_ori_total_budget_sql = ''' select coalesce(sum(budget),0.0) as total_budget 
                                from prod_smt_stg.symphony_budget_pitfill 
                                where concat_ws('-',year,month,day) = "{cur_day}"
                                and pre_date in ({st_end_times_str})
                                and city_id in ({city_list})
                                and trace_id= '123456'
                                and type = 'B'  '''

b_fillup_sql = '''
                       select  
                            city_id
                            ,pre_date as pred_date
                            ,sum(budget) as ori_b_budget
                        from prod_smt_stg.symphony_budget_pitfill
                        where   concat_ws('-',year,month,day) = date_sub("{cur_day}", {b_fillup_try_num} + 2)
                        and trace_id= '123456'
                        and type = 'B'
                        and city_id in ({city_list})
                        group by city_id,pre_date
                   '''

b_elasticity_sql = '''
                    select city_id,
                    cast(b_rate*1000 as int) as new_b_rate,
                    estimate delta_tsh
                    from gulfstream.sinan_online_predict_public_function_estimate
                    where incentive_type="city_daily_tshrate_BC_pangu_60"
                    and dt = date_sub('{cur_day}',{b_elasticity_try_num})

                    and model_version="v1.4_1.0.1"
                    and target_name = "pre_tsh_rate"
                    and   cast(b_rate*1000 as int) > 0
                    union all
                    select distinct city_id
                        ,0 as new_b_rate
                        ,0.0 as delta_tsh
                    from gulfstream.sinan_online_predict_public_function_estimate
                    where incentive_type="city_daily_tshrate_BC_pangu_60"
                    and dt = date_sub('{cur_day}',{b_elasticity_try_num})

                    and model_version="v1.4_1.0.1"
                    and target_name = "pre_tsh_rate"
                '''

# 【b端和c端表信息】
# b端和c端表信息】
sum_sql = '''
            select  
            a.city_id,
            a.pred_date,
            a.total_gmv,
            a.trace_id,
            a.treat_subsidy_hufan_rate as ori_c_rate,
            a.treat_subsidy_hufan_rate_new new_c_rate,
            a.pred_dsend_ratio pred_dsend_ratio,
            a.budget as ori_c_budget,
            a.realloc_budget_c as budget_c_gap, -- C端用于分配的金额
            a.ori_b_budget as ori_b_budget,
            a.ori_b_rate as ori_b_rate,
            a.ori_acc_budget as ori_acc_budget,
            a.ori_acc_rate as ori_acc_rate,
            a.pangu_stg_detail as pangu_stg_detail,
            a.acc_card_stg_detail as acc_card_stg_detail,
            a.ori_b_budget+a.realloc_b_budget as new_b_budget,
            (a.realloc_b_budget) as budget_b_gap, --除去填坑留下的 B端用于分配的金额
            a.new_b_rate as new_b_rate,
            a.subsidy_money_lowerbound as subsidy_money_lowerbound,
            a.subsidy_money_upperbound as subsidy_money_upperbound,
            a.lastweek_avg_diaodu_rate as lastweek_avg_diaodu_rate,
            a.lastweek_diaodu_gmv as lastweek_diaodu_gmv,   
            b.online_time base_online_time,
            b.asp asp,
            b.call_order_cnt call_order_cnt,
            b.call_order_cnt*(1+a.pred_dsend_ratio) new_call_order_cnt,
            (b.online_time*(1+a.delta_tsh)) new_online_time,
            b.strive_order_cnt strive_order_cnt, 
            c.type,
            c.k1,
            c.k2,
            c.k3,
            c.alpha,
            c.intercept,
            c.gongxu_max,
            c.gongxu_min,
            c.diaodu_b_max,
            c.diaodu_b_min,
            (b.online_time*(1+a.delta_tsh))/(b.call_order_cnt*(1.+a.pred_dsend_ratio)) as gongxu   

            from
            (
                select distinct 
                    *
                from ori_table
            ) a
            left join
            (
            {pred_sql}
            ) b
            on a.city_id = b.city_id
            and a.pred_date = b.pred_date
            left join
            (
            {alpha_sql}
            ) c
            on a.city_id = c.city_id
        '''

gongxu_sql = '''
           select avg(gongxu) gongxu_val
           from( select 
                stat_date as pred_date,
                sum(if(metric='total_gmv', value, 0)) as total_gmv,
                sum(if(metric='online_time', value, 0)) / sum(if(metric='total_no_call_order_cnt', value, 0)) as gongxu,
                sum(if(metric='online_time', value, 0)) as online_time,
                sum(if(metric='total_finish_order_cnt', value, 0)) as finish_order_cnt,
                coalesce(sum(if(metric='total_gmv', value, 0))/sum(if(metric='total_finish_order_cnt', value, 0)),10.0) as asp
                ,sum(if(metric='total_no_call_order_cnt', value, 0)) as call_order_cnt
                ,sum(if(metric='strive_order_cnt', value, 0)) as strive_order_cnt
            from riemann.daily_predict_w
            where concat_ws('-', year, month, day) = date_sub('{cur_day}',{try_num}+2)
            and stat_date in ({st_end_times_str})
            and city_id < 150
            and product = 'wyc'
            group by stat_date)
            '''

diaodu_sql = '''
select 
       ori.city_id as city_id,
       ori.day_of_week as day_of_week,
       coalesce(if(diaodu_range.subsidy_money_lowerbound < diaodu_range_default.subsidy_money_lowerbound_month, diaodu_range.subsidy_money_lowerbound,if (diaodu_range_default.subsidy_money_lowerbound_month < 0.5 * diaodu_range_default.subsidy_money_upperbound_month , diaodu_range_default.subsidy_money_lowerbound_month, 0.5*diaodu_range_default.subsidy_money_upperbound_month ) ), 0.0) as subsidy_money_lowerbound,
       coalesce(if(diaodu_range.subsidy_money_upperbound > diaodu_range_default.subsidy_money_upperbound_month, diaodu_range.subsidy_money_upperbound,diaodu_range_default.subsidy_money_upperbound_month ), 1000.0) as subsidy_money_upperbound,
       coalesce(diaodu_gmv_info.diaodu_subsidy,0.0) lastweek_diaodu_subsidy,
       coalesce(lastweek_avg_info.lastweek_avg_diaodu_rate,0.0) lastweek_avg_diaodu_rate,
       coalesce(diaodu_gmv_info.diaodu_gmv,1e-14) lastweek_diaodu_gmv
from
(
    select city_id,
          --dayofweek(pred_date) as day_of_week
          pmod(datediff(pred_date, '1920-01-01') - 3, 7) as day_of_week
    from df_sample
) ori
left join
(
    select 
            --dayofweek(dt) as day_of_week, 
            pmod(datediff(dt, '1920-01-01') - 3, 7) as day_of_week,
            cast(city_id as int) city_id, 
            sum(subsidy_diaodu) diaodu_subsidy,
            sum(gmv) as diaodu_gmv
    from  intelligence_da.wyg_dispatch_yield_space_ab 
    where dt between date_sub('{cur_day}',{try_num}+8) and date_sub('{cur_day}',{try_num} + 2)
    and city_id in ({city_list})
    and exp_group = 'control_group'
   group by pmod(datediff(dt, '1920-01-01') - 3, 7), city_id
) diaodu_gmv_info
on ori.city_id = diaodu_gmv_info.city_id
and ori.day_of_week = diaodu_gmv_info.day_of_week
left join
(
    select 
            --dayofweek(riqi) as day_of_week
            pmod(datediff(riqi, '1920-01-01') - 3, 7) as day_of_week
            ,city_id
            ,avg(float(subsidy_money_lowerbound)) as subsidy_money_lowerbound
            ,avg(float(subsidy_money_upperbound)) as subsidy_money_upperbound
    from stg_gs.dispatch_all_city_roi_data_daliy
    where riqi between date_sub('{cur_day}',{try_num}+8) and date_sub('{cur_day}',{try_num} + 2)
    group by pmod(datediff(riqi, '1920-01-01') - 3, 7),city_id
) diaodu_range 
on  ori.day_of_week =  diaodu_range.day_of_week
and ori.city_id = diaodu_range.city_id
left join
(
    select   city_id
            ,sum(subsidy_diaodu)/ sum(gmv) lastweek_avg_diaodu_rate
    from intelligence_da.wyg_dispatch_yield_space_ab
    where dt between date_sub('{cur_day}',{try_num}+8) and date_sub('{cur_day}',{try_num} + 2)
    and city_id in ({city_list})
    and exp_group = 'control_group'
    group by city_id
) lastweek_avg_info
on ori.city_id = lastweek_avg_info.city_id
--add default value
left join
(
         select ddtmprange.city_id, ddtmprange.diaodu_b_max * predgmv.total_gmv as subsidy_money_upperbound_month, 
         ddtmprange.diaodu_b_min * predgmv.total_gmv as subsidy_money_lowerbound_month from 
            (select
                city_id,
                cast(get_json_object(params, '$.diaodu_b_max') as double) as diaodu_b_max,
                cast(get_json_object(params, '$.diaodu_b_min') as double) as diaodu_b_min
            from bigdata_driver_ecosys_test.diaodu_gongxu_finish_rate_model_params_v2 
            where dt = date_sub('{cur_day}',{alpha_try_num})
            ) ddtmprange
            inner join
            (
              select 
                    city_id,
                    avg( value) as total_gmv
                from riemann.daily_predict_w
                where concat_ws('-',year,month,day) = date_sub('{cur_day}',{pred_try_num}+2)
                and stat_date in ({st_end_times_str})
                and product = 'wyc'
                and metric = 'total_gmv'
                and city_id in ({city_list})
                group by city_id
            )predgmv
            on ddtmprange.city_id = predgmv.city_id

) diaodu_range_default
on lastweek_avg_info.city_id = diaodu_range_default.city_id
'''
lastweek_diaodu_sql = '''
select
    tracker.ymd as tdate,
    tracker.city_id as city_id,
    coalesce(tracker.subsidy, 0.0) as last_week_subsidy,
    order_gmv.gmv as lastweek_diaodu_gmv
    --coalesce(tracker.subsidy, 0.0)/order_gmv.gmv as lastweek_avg_diaodu_rate
    from
    (
        select
            ymd,
            -- 日期
            int(city) as city_id,
            -- 城市 ID
            sum(subsidy) as subsidy -- 补贴金额，单位元
        from
            (
                select
                    param ['city'] as city,
                    -- 城市
                    concat_ws('-', year, month, day) as ymd,
                    -- 日期
                    param ['subsidy'] as subsidy
                from
                    engine_dmaster.ods_log_g_dmtracker_finish_dispatch_driver_feature -- 调度结束 tracker 表
                where
                    concat_ws('-', year, month, day) between date_sub('{cur_day}', '{diff_num}'+'{try_num}')and date_sub('{cur_day}', 1+'{try_num}') -- 限制日期
                    and param ['is_test'] = 0 -- test 为 1，代表调度没有播出去
                    and int(param ['city']) in ({city_list})
                    and param ['msg_id'] = "driver_dispatch_kc_common" -- 限制听不到单调度
            ) a
        group by
            ymd,
            city
    ) tracker
    left join (
        -- 选择数据列
        select
            
            dt as ymd,
            -- 发单日期
            int(call_city) as city_id,
            -- 发单城市
            round(sum(gmv_amt / 100), 2) as gmv -- 发单 GMV 金额，单位元
        from
            decdm.dwd_gflt_ord_order_base_di
        where
            dt between date_sub('{cur_day}', '{diff_num}'+'{try_num}')and date_sub('{cur_day}', 1+'{try_num}')
            and is_cross_city_flag != 1 -- 非跨城订单
            and is_td_finish_flag = 1 -- 当日完单
            and level_1_product = 110000 -- 仅网约车
        group by
            dt,
            call_city
    ) order_gmv on tracker.city_id = order_gmv.city_id
    and tracker.ymd = order_gmv.ymd
    order by
    tdate,
    city_id
'''
