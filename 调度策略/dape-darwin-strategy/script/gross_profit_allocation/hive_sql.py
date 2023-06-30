#!/usr/bin/env python
# -*- encoding: utf-8 -*-
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

# b端和c端表信息】
sum_sql = '''
select  
    a.city_id,
    a.pred_date,
    cast(a.total_gmv as float) as total_gmv,
    a.trace_id,
    a.treat_subsidy_hufan_rate as ori_c_rate,
    a.treat_subsidy_hufan_rate_new new_c_rate,
    a.pred_dsend_ratio pred_dsend_ratio,
    a.budget as ori_c_budget,
    a.realloc_budget_c as budget_c_gap, -- C端用于分配的金额
    a.ori_b_budget as ori_b_budget,
    a.ori_b_rate as ori_b_rate,
    0.0 as ori_acc_budget,
    0.0 as ori_acc_rate,
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

# base_c_tb
pred_and_c_sql = '''
    select
        ori_tb.city_id
        ,ori_tb.pred_date
        ,pmod(datediff(ori_tb.pred_date, '1920-01-01') - 3, 7) as day_of_week
        ,coalesce(pred_info.total_gmv,0.0) as total_gmv
        ,coalesce(pred_info.pukuai_gmv,0.0) as pukuai_gmv
        , 0.0 as treat_subsidy_hufan_rate -- 去除C端填坑
        , 0.0 as budget  -- 去除C端填坑
        ,'123' as trace_id
    from
    (
        select * 
        from df_sample
    ) ori_tb
    left join
    (
    {pred_sql}
    )pred_info
    on ori_tb.pred_date = pred_info.pred_date
    and ori_tb.city_id = pred_info.city_id    
'''

# B端分日弹性
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
# 原供需数据
pred_sql = '''
select a.pred_date,
a.city_id,
a.total_gmv as total_gmv,
a.online_time as online_time,
a.finish_order_cnt as finish_order_cnt,
a.asp as asp,
a.call_order_cnt as call_order_cnt,
coalesce(b.total_gmv,a.total_gmv/3, 0.0) as pukuai_gmv
from
(
    select
        estimate_date as pred_date,
        city_id,
        sum(case when key_id = 'gmv' then estimate_value else 0.0 end ) as total_gmv,
        sum( case when key_id = 'total_tsh' then estimate_value else 0.0 end ) as online_time,
        sum( case when key_id = 'finish_order_cnt' then estimate_value else 0.0 end ) as finish_order_cnt,
        coalesce( 
            sum( case when key_id = 'gmv' then estimate_value else 0.0 end ) / sum( case when key_id = 'finish_order_cnt' then estimate_value else 0.0 end ),
            10.0
        ) as asp,
        sum( case when key_id = 'compete_call_cnt' then estimate_value else 0.0 end ) as call_order_cnt
    from
        mp_data.app_trip_mkt_supply_demand_forecast_result_di
    where
        dt = date_sub("{gmv_dt}", 0) -- 指定的 gmv dt 分区
        and estimate_date in ({st_end_times_str})
        and product_name = '网约车'
        and city_id in ({city_list})
    group by
        estimate_date,
        city_id
) a
left join
(
    select city_id, 
        estimate_date as pred_date,
        sum( case when key_id = 'gmv' then estimate_value else 0.0 end ) as total_gmv,
        sum(case when key_id = 'total_tsh' then estimate_value  else 0.0 end) as online_time,
        sum(case when key_id = 'finish_order_cnt' then estimate_value  else 0.0 end ) as total_finish_order_cnt, 
        coalesce(sum(case when key_id = 'gmv' then estimate_value  else 0.0 end)/sum(case when key_id = 'finish_order_cnt' then estimate_value else 0.0 end),10.0) as asp,
        sum(case when key_id = 'compete_call_cnt' then estimate_value else 0.0 end) as call_order_cnt
        from mp_data.app_trip_mkt_supply_demand_forecast_result_di
    where dt = date_sub("{gmv_dt}", 0) -- 指定的 gmv dt 分区
    and estimate_date in ({st_end_times_str})
    and product_name = '快车'
    and city_id in ({city_list})
    group by
        city_id,
        estimate_date
)b
on a.pred_date=b.pred_date
and a.city_id=b.city_id
'''

# 原完单率模型参数
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

# 供需诊断数据
merge_all_sql = """
select
    a.city_id,
    a.pred_date,
    a.total_gmv,
    a.trace_id,
    a.treat_subsidy_hufan_rate as ori_c_rate,
    a.treat_subsidy_hufan_rate_new new_c_rate,
    a.pred_dsend_ratio pred_dsend_ratio,
    a.budget as ori_c_budget,
    a.realloc_budget_c as budget_c_gap,
    -- C端用于分配的金额
    a.ori_b_budget as ori_b_budget,
    a.ori_b_rate as ori_b_rate,
    a.ori_acc_budget as ori_acc_budget,
    a.ori_acc_rate as ori_acc_rate,
    a.pangu_stg_detail as pangu_stg_detail,
    a.acc_card_stg_detail as acc_card_stg_detail,
    a.ori_b_budget + a.realloc_b_budget as new_b_budget,
    (a.realloc_b_budget) as budget_b_gap,
    --除去填坑留下的 B端用于分配的金额
    a.new_b_rate as new_b_rate,
    a.subsidy_money_lowerbound as subsidy_money_lowerbound,
    a.subsidy_money_upperbound as subsidy_money_upperbound,
    a.lastweek_avg_diaodu_rate as lastweek_avg_diaodu_rate,
    a.lastweek_diaodu_gmv as lastweek_diaodu_gmv,
    b.online_time base_online_time,
    b.asp asp,
    b.call_order_cnt call_order_cnt,
    b.call_order_cnt *(1 + a.pred_dsend_ratio) new_call_order_cnt,
    (b.online_time *(1 + a.delta_tsh)) new_online_time,
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
    (b.online_time *(1 + a.delta_tsh)) /(b.call_order_cnt *(1.+ a.pred_dsend_ratio)) as gongxu
from
    (
        select
            city_id,
            pred_date
        from
            df_sample
    ) a
    left join ({ pred_info_sql }) b on a.city_id = b.city_id
    and a.pred_date = b.pred_date
    left join ({ alpha_sql }) c on a.city_id = c.city_id

"""

pred_info_sql = """
select
    a.city_id,
    a.stat_date,
    a.gmv as gmv_k,
    b.gmv as gmv_th,
    a.total_tsh as tsh_k,
    b.total_tsh as tsh_th,
    a.finish_order_cnt as finish_order_k, 
    b.finish_order_cnt as finish_order_th,
    a.asp as asp_k, 
    b.asp as asp_th,
    a.call as call_k, 
    b.call as call_th 
from
    (
        select
            city_id,
            stat_date,
            gmv,
            total_tsh,
            finish_order_cnt,
            coalesce(gmv/finish_order_cnt,10.0) as asp,
            compete_call_cnt as call 
        from
            mp_data.dm_trip_mp_sd_core_1d
        where
            dt between '{start_time}'
            and '{end_time}'
            and product_id = 110100
    ) a
    left join (
        select
            city_id,
            stat_date,
            gmv,
            total_tsh,
            finish_order_cnt,
            coalesce(gmv/finish_order_cnt,10.0) as asp,
            compete_call_cnt as call 
        from
            mp_data.dm_trip_mp_sd_core_1d
        where
            dt between '{start_time}'
            and '{end_time}'
            and product_id = 110103
    ) b on a.city_id = b.city_id
    and a.stat_date = b.stat_date
"""

# B端供需诊断数据
gap_dignosis_sql = '''
select
    city_id,
    result_date as stat_date,
    fence_id,
    tool,
    strategy_type,
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
    concat_ws('-', year, month, day) = date_sub("{cr_dt}",0)
    and strategy_type in ({fixed_threshold_str})
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
        fake_b_ela_table
) elestic_pangu_df 
    inner join (
    -- pred hourly base info
    select
        city_id,
        estimate_date as stat_date,
        floor(hour / 2) hour,
        sum(
            case
                when key_id = 'gmv' then key_value
                else 0.0
            end
        ) as total_gmv,
        sum(
            case
                when key_id = 'online_dur' then key_value
                else 0.0
            end
        ) as online_time,
        sum(
            case
                when key_id = 'objective_call_cnt' then key_value
                else 0.0
            end
        ) * sum(
            case
                when key_id = 'objective_exp_openapi_pp' then key_value
                else 0.0
            end
        ) as total_finish_order_cnt,
        coalesce(
            sum(
                case
                    when key_id = 'gmv' then key_value
                    else 0.0
                end
            ) /(
                sum(
                    case
                        when key_id = 'objective_call_cnt' then key_value
                        else 0.0
                    end
                ) * sum(
                    case
                        when key_id = 'objective_exp_openapi_pp' then key_value
                        else 0.0
                    end
                )
            ),
            10.0
        ) as asp,
        sum(
            case
                when key_id = 'objective_call_cnt' then key_value
                else 0.0
            end
        ) as call_order_cnt
    from
        mp_data.app_trip_mp_sd_hour_forecast_di
    where
        dt = date_sub("{gmv_dt}",0)
        and  estimate_date in ({st_end_times_str})  --时间改
        and city_id < 400
        and product_name = '泛快含优含拼（快车（普通快车、A+、车大、巡游网约车）、市内拼车、优享、D1、特惠快车、涅槃）'
    group by
        city_id,
        estimate_date,
        floor(hour / 2)
    ) pred_df 
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
            fake_b_ela_table
        ) elestic_acc_card_df
        inner join (
       -- pred hourly base info
        select
            city_id,
            estimate_date as stat_date,
            floor(hour / 2) hour,
            sum(
                case
                    when key_id = 'gmv' then key_value
                    else 0.0
                end
            ) as total_gmv,
            sum(
                case
                    when key_id = 'online_dur' then key_value
                    else 0.0
                end
            ) as online_time,
            sum(
                case
                    when key_id = 'objective_call_cnt' then key_value
                    else 0.0
                end
            ) * sum(
                case
                    when key_id = 'objective_exp_openapi_pp' then key_value
                    else 0.0
                end
            ) as total_finish_order_cnt,
            coalesce(
                sum(
                    case
                        when key_id = 'gmv' then key_value
                        else 0.0
                    end
                ) /(
                    sum(
                        case
                            when key_id = 'objective_call_cnt' then key_value
                            else 0.0
                        end
                    ) * sum(
                        case
                            when key_id = 'objective_exp_openapi_pp' then key_value
                            else 0.0
                        end
                    )
                ),
                10.0
            ) as asp,
            sum(
                case
                    when key_id = 'objective_call_cnt' then key_value
                    else 0.0
                end
            ) as call_order_cnt
        from
            mp_data.app_trip_mp_sd_hour_forecast_di
        where
            dt = date_sub("{gmv_dt}",0)
            and  estimate_date in ({st_end_times_str})  --时间改
            and city_id < 400
            and product_name = '泛快含优含拼（快车（普通快车、A+、车大、巡游网约车）、市内拼车、优享、D1、特惠快车、涅槃）'
        group by
            city_id,
            estimate_date,
            floor(hour / 2)
       ) pred_df 
        on elestic_acc_card_df.city_id = pred_df.city_id
        and elestic_acc_card_df.start_dt = pred_df.stat_date
        and cast(elestic_acc_card_df.start_time as int) = cast(pred_df.hour as int)
'''


pred_and_pangu_elastic_sql_old = '''
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
        dt = date_sub("{gmv_dt}",0)
        and estimate_date in ({st_end_times_str})
        and product_name = '泛快含优不含拼（快车（普通快车、A+、车大、巡游网约车）、优享、D1、特惠快车、涅槃）'
    group by
        city_id,
        estimate_date,
        hour) pred_df 
on elestic_pangu_df.city_id = pred_df.city_id
and elestic_pangu_df.start_dt = pred_df.stat_date
and cast(elestic_pangu_df.start_time as int) = cast(pred_df.hour as int)
'''

# 加速卡弹性和供需数据
pred_and_acc_elastic_sql_old = '''
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
        select
            city_id,
            start_dt,
            start_dt as end_dt,
            start_time,
            start_time as end_time,
            b_rate,
            estimate
        from
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
                dt = date_sub("{gmv_dt}",0)
                and estimate_date in ({st_end_times_str})
                and product_name = '泛快含优不含拼（快车（普通快车、A+、车大、巡游网约车）、优享、D1、特惠快车、涅槃）'
            group by
                city_id,
                estimate_date,
                hour) pred_df 
        on elestic_acc_card_df.city_id = pred_df.city_id
        and elestic_acc_card_df.start_dt = pred_df.stat_date
        and cast(elestic_acc_card_df.start_time as int) = cast(pred_df.hour as int)
'''

asp_sql = '''
select
    city_id,
    avg(coalesce(gmv/finish_order_cnt,10.0)) as asp
from
    mp_data.dm_trip_mp_sd_core_1d
where
    dt between date_sub('{cur_day}',9) and date_sub('{cur_day}',2)
    and product_id = '110100_110400_110800_110103'
group by city_id
'''

forecast_daily_for_b_fillup_sql = '''
select
    city_id,
    estimate_date as stat_date,    
    sum(
        case
            when key_id = 'finish_order_cnt' then estimate_value
            else 0.0
        end
    ) as total_finish_order_cnt,
    sum(
        case
            when key_id = 'objective_call_cnt' then estimate_value
            else 0.0
        end
    ) as call_order_cnt,
    sum(
        case
            when key_id = 'gmv' then estimate_value
            else 0.0
        end
    )as total_gmv,
    coalesce(sum(
        case
            when key_id = 'gmv' then estimate_value
            else 0.0
        end
    )/sum(
        case
            when key_id = 'finish_order_cnt' then estimate_value
            else 0.0
        end
    ),10) as asp
from
    mp_data.app_trip_mkt_supply_demand_forecast_result_di
where
    dt = date_sub("{gmv_dt}",0) -- 指定的 gmv dt 分区
    and estimate_date in ({st_end_times_str})
    -- and product_name = '泛快含优不含拼（快车（普通快车、A+、车大、巡游网约车）、优享、D1、特惠快车、涅槃）'
    and product_name = '泛快含优含拼（快车（普通快车、A+、车大、巡游网约车）、市内拼车、优享、D1、特惠快车、涅槃）'
group by
    city_id,
    estimate_date
'''
exp_forecast_daily_for_b_fillup_sql = '''
select
    call_table.city_id,
    call_table.stat_date,
    gmv_table.total_gmv,
    call_table.total_finish_order_cnt,
    call_table.call_order_cnt,
    coalesce(gmv_table.total_gmv/call_table.total_finish_order_cnt,10) as asp
from
    (
        select
            city_id,
            stat_date,
            sum(total_finish_order_cnt) as total_finish_order_cnt,
            sum(call_order_cnt) as call_order_cnt
        from(
                select
                    fkp_table.city_id,
                    fkp_table.stat_date,
                    fkp_table.total_gmv as total_gmv,
                    fkp_table.call_order_cnt * fknp_exp_cr.cr as total_finish_order_cnt,
                    fkp_table.call_order_cnt as call_order_cnt
                from
                    (
                       select
                        city_id,
                        estimate_date as stat_date,
                        cast(hour as int)  as half_hour,
                        sum(
                            case
                                when key_id = 'gmv' then key_value
                                else 0.0
                            end
                        ) as total_gmv,
                    
                        sum(
                            case
                                when key_id = 'objective_call_cnt' then key_value
                                else 0.0
                            end
                        ) * sum(
                            case
                                when key_id = 'objective_exp_openapi_pp' then key_value
                                else 0.0
                            end
                        ) as total_finish_order_cnt,
                        sum(
                            case
                                when key_id = 'objective_call_cnt' then key_value
                                else 0.0
                            end
                        ) as call_order_cnt
                    from
                        mp_data.app_trip_mp_sd_hour_forecast_di
                    where
                        dt =  date_sub("{gmv_dt}",0)
                        and   estimate_date in ({st_end_times_str})  
                        and city_id < 400
                        and product_name = '泛快含优含拼（快车（普通快车、A+、车大、巡游网约车）、市内拼车、优享、D1、特惠快车、涅槃）'
                    group by
                        city_id,
                        estimate_date,
                        hour
                    ) fkp_table
                    left join(
                        select
                            result_date as stat_date,
                            city_id,
                            minute30 as half_hour,
                            cr
                        from
                            gulfstream.offline_gongxu_pred_minute30_result
                        where
                            dt = date_sub("{cr_dt}",0)
                            and product_id = "110100_110400_110800_110103"
                            and version = 'exp'
                            and fence_id = -1
                            and result_date in ({st_end_times_str})
                    ) fknp_exp_cr on fkp_table.city_id = fknp_exp_cr.city_id
                    and fkp_table.stat_date = fknp_exp_cr.stat_date
                    and fkp_table.half_hour = fknp_exp_cr.half_hour
            )
        group by
            city_id,
            stat_date
    ) call_table
    left join (
        select
            city_id,
            estimate_date as stat_date,
            sum(
                case
                    when key_id = 'gmv' then estimate_value
                    else 0.0
                end
            ) as total_gmv
        from
            mp_data.app_trip_mkt_supply_demand_forecast_result_di
        where
            dt = date_sub("{gmv_dt}",0)
            and estimate_date in ({st_end_times_str})
             -- and product_name = '泛快含优不含拼（快车（普通快车、A+、车大、巡游网约车）、优享、D1、特惠快车、涅槃）'
            and product_name = '泛快含优含拼（快车（普通快车、A+、车大、巡游网约车）、市内拼车、优享、D1、特惠快车、涅槃）'
        group by
            city_id,
            estimate_date
    ) gmv_table on call_table.city_id == gmv_table.city_id
    and call_table.stat_date == gmv_table.stat_date
'''

forecast_call_finish_sql = '''
select
    city_id,
    estimate_date as stat_date,    
    sum(
        case
            when key_id = 'finish_order_cnt' then estimate_value
            else 0.0
        end
    ) as total_finish_order_cnt,
  
    sum(
        case
            when key_id = 'compete_call_cnt' then estimate_value
            else 0.0
        end
    ) as call_order_cnt
from
    mp_data.app_trip_mkt_supply_demand_forecast_result_di
where
    dt = date_sub("{gmv_dt}",0) -- 指定的 gmv dt 分区
    and estimate_date in ({st_end_times_str})
    and product_name = '网约车'
group by
    city_id,
    estimate_date
'''

wyc_asp_sql = '''
select
    city_id,
    estimate_date as stat_date,
    sum(
        case
            when key_id = 'gmv' then estimate_value
            else 0.0
        end
    ) as total_gmv,
    sum(case when key_id = 'total_tsh' then estimate_value  else 0.0 end) as online_time,
    sum(case when key_id = 'finish_order_cnt' then estimate_value  else 0.0 end ) as finish_order_cnt,
    coalesce(sum(case when key_id = 'gmv' then estimate_value  else 0.0 end)/sum(case when key_id = 'finish_order_cnt' then estimate_value else 0.0 end),10.0) as asp
from
    -- riemann.daily_predict_w
    mp_data.app_trip_mkt_supply_demand_forecast_result_di
where
    dt = date_sub("{gmv_dt}", 0) -- 指定的 gmv dt 分区
    and estimate_date in ({st_end_times_str})
    and product_name = '网约车'
group by
    estimate_date,
    city_id
'''
# 历史TR和一口价信息
pred_and_tr_ykj_info_sql = """
select
    distinct
    gmv_info.city_id,
    gmv_info.stat_date,
    cast(gmv_info.gmv_k as double) as gmv_k,
    cast(gmv_info.gmv_pk as double) as gmv_pk,
    cast(gmv_info.gmv_th as double) as gmv_th,
    tr_info.day_of_week,
    tr_info.take_rate_k,
    tr_info.take_rate_th,
    ykj_info.ykj_th,
    cast(gmv_info.tsh_k as float) as tsh_k,
    cast(gmv_info.tsh_th as float) as tsh_th,
    cast(gmv_info.finish_order_k as float) as finish_order_k,
    cast(gmv_info.finish_order_th as float) as finish_order_th,
    cast(gmv_info.asp_k as float) as asp_k,
    cast(gmv_info.asp_th as float) as asp_th,
    cast(gmv_info.call_k_cpt as float) as call_k_cpt,
    cast(gmv_info.call_k_obj as float) as call_k_obj,
    cast(gmv_info.call_th_cpt as float) as call_th_cpt,
    cast(gmv_info.call_th_obj as float) as call_th_obj
from
    (
    select
        distinct 
        aa.city_id,
        aa.stat_date as stat_date,
        coalesce(ab.gmv_k, st.gmv_k, 0 ) as gmv_k,
        coalesce(ab.gmv_k, st.gmv_pk, 0 ) as gmv_pk,
        coalesce(ab.gmv_th, st.gmv_th , 0 ) as gmv_th,
        coalesce(ab.tsh_k,st.tsh_k, 0 ) as tsh_k,
        coalesce(ab.tsh_th, st.tsh_th, 0 ) as tsh_th,
        coalesce(ab.finish_order_k,st.finish_order_k, 0 ) as finish_order_k,
        coalesce(ab.finish_order_th, st.finish_order_th, 0 ) as finish_order_th,
        coalesce(ab.asp_k, st.asp_k , 10) as asp_k,
        coalesce(ab.asp_th, st.asp_th, 10) as asp_th,
        coalesce(ab.call_k_cpt, st.call_k_cpt, 0 ) as call_k_cpt,
        coalesce(ab.call_k_obj, st.call_k_obj, 0 ) as call_k_obj,

        coalesce(ab.call_th_cpt, st.call_th_cpt, 0 ) as call_th_cpt,
        coalesce(ab.call_th_obj, st.call_th_obj , 0) as call_th_obj
    from
    (select distinct 
            city_id ,
            pred_date as stat_date 
        from df_sample
    )aa 
    left join 
        (   -- 获取核销GMV数据【快车&特惠】
            select distinct
                a.city_id,
                a.stat_date,
                a.gmv as gmv_k,
                b.gmv as gmv_th,
                a.total_tsh as tsh_k,
                b.total_tsh as tsh_th,
                a.finish_order_cnt as finish_order_k, 
                b.finish_order_cnt as finish_order_th,
                a.asp as asp_k,  
                b.asp as asp_th,
                a.call_cpt as call_k_cpt, 
                b.call_cpt as call_th_cpt,
                a.call_obj as call_k_obj, 
                b.call_obj as call_th_obj

            from
                (
                    select 
                        city_id,
                        stat_date,
                        gmv,
                        total_tsh,
                        finish_order_cnt,
                        coalesce(gmv/finish_order_cnt,10.0) as asp,
                        compete_call_cnt as call_cpt,
                        objective_call_cnt as call_obj
                    from
                        mp_data.dm_trip_mp_sd_core_1d
                    where
                        dt between "{start_time}"
                        and "{end_time}"
                        and product_id = 110100
                ) a
                left join (
                    select 
                        city_id,
                        stat_date,
                        gmv,
                        total_tsh,
                        finish_order_cnt,
                        coalesce(gmv/finish_order_cnt,10.0) as asp,
                        compete_call_cnt as call_cpt,
                        objective_call_cnt as call_obj
                    from
                        mp_data.dm_trip_mp_sd_core_1d
                    where
                        dt between "{start_time}"
                        and "{end_time}"
                        and product_id = 110103
                ) b on a.city_id = b.city_id
                and a.stat_date = b.stat_date
        ) ab   
        on aa.city_id = ab.city_id
        and aa.stat_date = ab.stat_date 
        left join (
            -- 获取预估GMV数据【快车&特惠】
            select distinct 
                s.city_id,
                s.stat_date,
                case when j.total_gmv < 10  then j.total_gmv else s.total_gmv end as gmv_k, -- 调整，因网约车口径有疫情策略
                case when j.total_gmv < 10  then j.total_gmv else k.total_gmv end as gmv_pk, 
                case when j.total_gmv < 10  then j.total_gmv else t.total_gmv end as gmv_th,
                s.online_time as tsh_k,
                t.online_time as tsh_th,
                case when j.total_finish_order_cnt < 10  then j.total_finish_order_cnt else s.total_finish_order_cnt end as finish_order_k, -- 调整，因网约车口径有疫情策略
                case when j.total_finish_order_cnt < 10  then j.total_finish_order_cnt else t.total_finish_order_cnt end as finish_order_th,
                -- s.total_finish_order_cnt as finish_order_k,
                -- t.total_finish_order_cnt as finish_order_th,
                s.asp as asp_k,
                t.asp as asp_th,
                case when j.call_order_cnt_cpt < 10  then j.call_order_cnt_cpt else s.call_order_cnt_cpt end as call_k_cpt, -- 调整，因网约车口径有疫情策略
                case when j.call_order_cnt_cpt < 10  then j.call_order_cnt_cpt else t.call_order_cnt_cpt end as call_th_cpt,
                -- s.call_order_cnt_cpt as call_k_cpt,
                -- t.call_order_cnt_cpt as call_th_cpt,
                case when j.call_order_cnt_obj < 10  then j.call_order_cnt_obj else s.call_order_cnt_obj end as call_k_obj, -- 调整，因网约车口径有疫情策略
                case when j.call_order_cnt_obj < 10  then j.call_order_cnt_obj else t.call_order_cnt_obj end as call_th_obj
                -- s.call_order_cnt_obj as call_k_obj,
                -- t.call_order_cnt_obj as call_th_obj

            from
                (
                    select  
                        city_id,
                        estimate_date as stat_date,
                        sum(
                            case
                                when key_id = 'gmv' then estimate_value
                                else 0.0
                            end
                        ) as total_gmv,
                        sum(case when key_id = 'total_tsh' then estimate_value  else 0.0 end) as online_time,
                        sum(case when key_id = 'finish_order_cnt' then estimate_value  else 0.0 end ) as total_finish_order_cnt,
                        coalesce(sum(case when key_id = 'gmv' then estimate_value  else 0.0 end)/sum(case when key_id = 'finish_order_cnt' then estimate_value else 0.0 end),10.0) as asp,
                        sum(case when key_id = 'compete_call_cnt' then estimate_value else 0.0 end) as call_order_cnt_cpt,
                        sum(case when key_id = 'objective_call_cnt' then estimate_value else 0.0 end) as call_order_cnt_obj

                    from
                        api_pred_info
                    where
                        estimate_date in ({st_end_times_str})
                        and product_name = '快车'
                    group by
                        city_id,
                        estimate_date
                ) s
                left join (
                    select 
                        city_id,
                        estimate_date as stat_date,
                        sum(
                            case
                                when key_id = 'gmv' then estimate_value
                                else 0.0
                            end
                        ) as total_gmv,
                        sum(case when key_id = 'total_tsh' then estimate_value  else 0.0 end) as online_time,
                        sum(case when key_id = 'finish_order_cnt' then estimate_value  else 0.0 end ) as total_finish_order_cnt,
                        coalesce(sum(case when key_id = 'gmv' then estimate_value  else 0.0 end)/sum(case when key_id = 'finish_order_cnt' then estimate_value else 0.0 end),10.0) as asp,
                        sum(case when key_id = 'compete_call_cnt' then estimate_value else 0.0 end) as call_order_cnt_cpt,
                        sum(case when key_id = 'objective_call_cnt' then estimate_value else 0.0 end) as call_order_cnt_obj
                    from
                        -- mp_data.app_trip_mkt_supply_demand_forecast_result_di
                        api_pred_info
                    where
                        -- dt = date_sub("{gmv_dt}", 0) -- 指定的 gmv dt 分区
                        estimate_date in ({st_end_times_str})
                        and product_name = '特惠自营'
                    group by
                        city_id,
                        estimate_date
                ) t on s.city_id = t.city_id
                    and s.stat_date = t.stat_date
                left join 
                (select 
                -- 增加网约车口径，主要是网约车口径有疫情策略，其他口径没有；如果有疫情情况下， 根据网约车口径对其他口径进行调整
                        city_id,
                        estimate_date as stat_date,
                        sum(
                            case
                                when key_id = 'gmv' then estimate_value
                                else 0.0
                            end
                        ) as total_gmv,
                        sum(case when key_id = 'total_tsh' then estimate_value  else 0.0 end) as online_time,
                        sum(case when key_id = 'finish_order_cnt' then estimate_value  else 0.0 end ) as total_finish_order_cnt,
                        coalesce(sum(case when key_id = 'gmv' then estimate_value  else 0.0 end)/sum(case when key_id = 'finish_order_cnt' then estimate_value else 0.0 end),10.0) as asp,
                        sum(case when key_id = 'compete_call_cnt' then estimate_value else 0.0 end) as call_order_cnt_cpt,
                        sum(case when key_id = 'objective_call_cnt' then estimate_value else 0.0 end) as call_order_cnt_obj
                    from
                        -- mp_data.app_trip_mkt_supply_demand_forecast_result_di
                        api_pred_info
                    where
                        -- dt = date_sub("{gmv_dt}", 0) -- 指定的 gmv dt 分区
                        estimate_date in ({st_end_times_str})
                        and product_name = '网约车'
                    group by
                        city_id,
                        estimate_date 
                )j on s.city_id = j.city_id
                    and s.stat_date = j.stat_date
                
                left join (
                    -- 增加普快口径的gmv 信息
                        select
                            city_id,
                            estimate_date as stat_date,
                            sum(
                                case
                                    when key_id = 'gmv' then estimate_value
                                    else 0.0
                                end
                            ) as total_gmv
                        from
                            -- mp_data.app_trip_mkt_supply_demand_forecast_result_di
                            api_pred_info
                        where
                            -- dt = date_sub("{gmv_dt}", 0) -- 指定的 gmv dt 分区
                            estimate_date in ({st_end_times_str})
                            and product_name == "快车+D1+涅槃（大盘GMV预测）"
                        group by
                            city_id,
                            estimate_date
                    ) k on s.city_id = k.city_id
                    and s.stat_date = k.stat_date
        )st 
        on aa.city_id = st.city_id
        and aa.stat_date = st.stat_date
    ) gmv_info
    left join (
        -- TR信息获取
        select 
            aa.city_id,
            aa.stat_date,
            aa.day_of_week,
            coalesce(bb.target_rate, coalesce(cc.yingfutr_th, 0.3)) as take_rate_th,
            coalesce(cc.yingfutr_kuai, 0.3) as take_rate_k
        from
            (
                select distinct
                    city_id,
                    pred_date as stat_date,
                    pmod(datediff(pred_date, '2018-01-01') - 6, 7) as day_of_week
                from
                    df_sample
            ) aa
            left join (
                select
                    city_id,
                    target_rate
                from
                    ppe.tehui_city_tr_target_rate
                where
                    dt = date_sub("{gmv_dt}", 0)
            ) bb on aa.city_id = bb.city_id
            left join (
                select
                    a.city_id,
                    pmod(datediff(a.pre_date, '2018-01-01') - 6, 7) as day_of_week,
                    --  0是周天,1是周一
                    avg(a.yingfutr_th) as yingfutr_th,
                    avg(b.yingfutr_kuai) as yingfutr_kuai
                from
                    (
                        select
                            city_id,
                            to_date(end_charge_time) as pre_date,
                            1 - sum(online_driver_divide) / sum(gmv) as yingfutr_th
                        from
                            ppe.tehui_caiwu_order_d
                        where
                            dt between date_sub("{gmv_dt}", 21)
                            and "{gmv_dt}"
                            and to_date(end_charge_time) > date_sub("{gmv_dt}", 21)
                            and product_category in ('快车')
                        group by
                            city_id,
                            to_date(end_charge_time)
                    ) as a
                    left join (
                        select
                            city_id,
                            to_date(end_charge_time) as pre_date,
                            1 - sum(online_driver_divide) / sum(gmv) as yingfutr_kuai
                        from
                            ppe.tehui_caiwu_order_d
                        where
                            dt between date_sub("{gmv_dt}", 21)
                            and "{gmv_dt}"
                            and to_date(end_charge_time) > date_sub("{gmv_dt}", 21)
                            and product_category in ('特惠快车')
                        group by
                            city_id,
                            to_date(end_charge_time)
                    ) as b on a.city_id = b.city_id
                    and a.pre_date = b.pre_date
                group by
                    a.city_id,
                    pmod(datediff(a.pre_date, '2018-01-01') - 6, 7) --  0是周天,1是周一
            ) cc on aa.city_id = cc.city_id
            and aa.day_of_week = cc.day_of_week
    ) tr_info on gmv_info.city_id = tr_info.city_id
    and gmv_info.stat_date = tr_info.stat_date
    left join (
        -- 一口价信息获取
        select
            aa.city_id,
            aa.stat_date,
            coalesce(bb.target_rate, 0.03) as ykj_th
        from
            (
                select distinct
                    city_id,
                    pred_date as stat_date
                from
                    df_sample
            ) aa
            left join (
                select
                    city_id,
                    target_rate
                from
                    ppe.tehui_city_flat_fare_target_rate_di
                where
                    dt = "{gmv_dt}" -- current_date
            ) bb on aa.city_id = bb.city_id
    ) ykj_info on gmv_info.city_id = ykj_info.city_id
    and gmv_info.stat_date = ykj_info.stat_date
"""

# 普快补贴率对泛快发单的变化
c_elasticity_sql = '''
select
    city_id,
    tdate as pred_date,
    cast(round(treat_subsidy_hufan_rate, 3) * 1000 as int) as treat_subsidy_hufan_rate_new,
    cast(
        get_json_object(prediction, '$.pred_dsend_ratio') as double
    ) as pred_dsend_ratio
from
    bigdata_driver_ecosys_test.tmp_hufan_delta_pred_dsend_v5_causal_inference2
where
    dt = date_sub('{gmv_dt}', {c_elasticity_try_num})
'''

# 调度补贴上下限
diaodu_sql = '''
select 
       ori.city_id as city_id,
       ori.day_of_week as day_of_week,
       coalesce(if(coalesce(diaodu_range.subsidy_money_lowerbound,0) < diaodu_range_default.subsidy_money_lowerbound_month, diaodu_range_default.subsidy_money_lowerbound_month, diaodu_range.subsidy_money_lowerbound), 0.0) as subsidy_money_lowerbound,
       --coalesce(if(diaodu_range.subsidy_money_lowerbound < diaodu_range_default.subsidy_money_lowerbound_month, diaodu_range.subsidy_money_lowerbound,if (diaodu_range_default.subsidy_money_lowerbound_month < 0.5 * diaodu_range_default.subsidy_money_upperbound_month , diaodu_range_default.subsidy_money_lowerbound_month, 0.5*diaodu_range_default.subsidy_money_upperbound_month ) ), 0.0) as subsidy_money_lowerbound,
       coalesce(if(coalesce(diaodu_range.subsidy_money_upperbound, 0) > diaodu_range_default.subsidy_money_upperbound_month, diaodu_range.subsidy_money_upperbound,diaodu_range_default.subsidy_money_upperbound_month), 1000.0) as subsidy_money_upperbound,
       coalesce(diaodu_gmv_info.diaodu_subsidy,0.0) lastweek_diaodu_subsidy,
       coalesce(lastweek_avg_info.lastweek_avg_diaodu_rate,0.0) lastweek_avg_diaodu_rate,
       coalesce(diaodu_gmv_info.diaodu_gmv,1e-14) lastweek_diaodu_gmv,
       subsidy_money_lowerbound_month,
       subsidy_money_upperbound_month
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
on ori.city_id = diaodu_gmv_info.city_id and ori.day_of_week = diaodu_gmv_info.day_of_week
left join
(
    select 
            --dayofweek(riqi) as day_of_week
            pmod(datediff(riqi, '1920-01-01') - 3, 7) as day_of_week
            ,city_id
            ,cast(coalesce(avg(float(subsidy_money_lowerbound)), 0) as Double) as subsidy_money_lowerbound
            ,cast(coalesce(avg(float(subsidy_money_upperbound)), 0) as Double) as subsidy_money_upperbound
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
                -- 添加补贴率约束上下界 [0.05% - 0.5%] 
                if( cast(get_json_object(params, '$.diaodu_b_max') as double) > 0.005,  0.005,  cast(get_json_object(params, '$.diaodu_b_max') as double)  ) as diaodu_b_max,
                if( cast(get_json_object(params, '$.diaodu_b_min') as double) < 0.0005, 0.0005, cast(get_json_object(params, '$.diaodu_b_min') as double) )  as diaodu_b_min
            from bigdata_driver_ecosys_test.diaodu_gongxu_finish_rate_model_params_v2 
            where dt = date_sub('{cur_day}',{alpha_try_num})
            ) ddtmprange
            inner join
            (
              select 
                    city_id,
                    avg( estimate_value) as total_gmv
                from mp_data.app_trip_mkt_supply_demand_forecast_result_di 
                where dt = date_sub("{gmv_dt}", 0) -- 指定的 gmv dt 分区
                and estimate_date in ({st_end_times_str})
                and product_name = '网约车'
                and key_id = 'gmv'
                and city_id in ({city_list})
                group by city_id
            )predgmv
            on ddtmprange.city_id = predgmv.city_id

) diaodu_range_default
on ori.city_id = diaodu_range_default.city_id
'''

# 基于毛利版本的C端多品类弹性
# c_elestic data 
c_elestic_pred_info_sql = """
select  
    aa.city_id,
    aa.pred_date,
    elestic_info.treat_subsidy_hufan_rate_pk,
    elestic_info.treat_subsidy_hufan_rate_th,
    coalesce(elestic_info.pred_dsend_ratio_k, 0.0) as pred_dsend_ratio_k,
    coalesce(elestic_info.pred_dsend_ratio_th, 0.0) as pred_dsend_ratio_th,
    coalesce(pred_info.gmv_k,0.0) as gmv_k,
    coalesce(pred_info.gmv_pk,0.0) as gmv_pk,
    coalesce(pred_info.gmv_th,0.0) as gmv_th,
    coalesce(pred_info.tsh_k, 0.0 ) as tsh_k,
    coalesce(pred_info.tsh_th, 0.0) as tsh_th,
    coalesce(pred_info.finish_order_k, 0 ) as finish_order_k,
    coalesce(pred_info.finish_order_th, 0 ) as finish_order_th,
    coalesce(pred_info.asp_k, 10.0) as asp_k,
    coalesce(pred_info.asp_th, 10.0) as asp_th,
    coalesce(pred_info.ykj_th, 0.03) as ykj_th,
    coalesce(pred_info.call_k_cpt, 0.0) as call_k_cpt,
    coalesce(pred_info.call_th_cpt, 0.0) as call_th_cpt,

    coalesce(pred_info.call_k_cpt * (1 + elestic_info.pred_dsend_ratio_k) , 0.0 ) as call_k_cpt_aft, 
    coalesce(pred_info.call_th_cpt * (1 + elestic_info.pred_dsend_ratio_th),0.0 ) as call_th_cpt_aft, 

    round(coalesce(round((tsh_k/(pred_info.call_k_cpt * (1 + elestic_info.pred_dsend_ratio_k)))/0.02, 0) * 0.02 , 0.0),2) as dsr_k_cpt,
    round(coalesce(round((tsh_th/(pred_info.call_th_cpt * (1 + elestic_info.pred_dsend_ratio_th)))/0.02, 0) * 0.02 , 0.0),2) as dsr_th_cpt
from
    (
        select distinct 
            city_id,
            pred_date
        from
            df_sample
    ) aa
    inner join (
        select  distinct
            a.city_id,
            a.pred_date,
            a.treat_subsidy_hufan_rate_pk,
            a.treat_subsidy_hufan_rate_th,
            a.pred_dsend_ratio_k,
            b.pred_dsend_ratio_th
        from
            (
                select  
                    city_id,
                    tdate as pred_date,
                    cast(round(pukuai_hufan_c_rate, 3) as float) as treat_subsidy_hufan_rate_pk,
                    cast(round(tehui_hufan_c_rate, 3)  as float) as treat_subsidy_hufan_rate_th,
                    cast(pred_dsend_ratio as double) as pred_dsend_ratio_k
                from
                    -- bigdata_driver_ecosys_test.tmp_kuai_tehui_dsend_pred
                    bigdata_driver_ecosys_test.profit_kuai_tehui_dsend_pred
                where
                    dt = date_sub("{gmv_dt}",{c_elestic_try_num}) 
                    and label = 'com_kuai_delta_send_ratio'
                    and model_version = 'doudi_manualfix'
            ) a
            left join (
                select
                    city_id,
                    tdate as pred_date,
                    cast(round(pukuai_hufan_c_rate, 3) as float) as treat_subsidy_hufan_rate_pk,
                    cast(round(tehui_hufan_c_rate, 3)  as float) as treat_subsidy_hufan_rate_th,
                    cast(pred_dsend_ratio as double) as pred_dsend_ratio_th
                from
                    -- bigdata_driver_ecosys_test.tmp_kuai_tehui_dsend_pred
                    bigdata_driver_ecosys_test.profit_kuai_tehui_dsend_pred
                where
                    dt = date_sub("{gmv_dt}",{c_elestic_try_num}) 
                    and label = 'com_tehui_delta_send_ratio'
                    and model_version = 'doudi_manualfix'
            ) b on a.city_id = b.city_id
            and a.pred_date = b.pred_date
            and a.treat_subsidy_hufan_rate_pk = b.treat_subsidy_hufan_rate_pk
            and a.treat_subsidy_hufan_rate_th = b.treat_subsidy_hufan_rate_th
    ) elestic_info on aa.city_id = elestic_info.city_id
    and aa.pred_date = elestic_info.pred_date
    inner join 
    (select distinct
        city_id,
        stat_date,
        gmv_k,
        gmv_pk,
        gmv_th,
        tsh_k,
        tsh_th,
        finish_order_k,
        finish_order_th,
        asp_k,
        asp_th,
        ykj_th,
        call_k_cpt,
        call_th_cpt,
        call_k_obj,
        call_th_obj
    from pred_and_tr_ykj_info)pred_info 
    on aa.city_id = pred_info.city_id
    and aa.pred_date = pred_info.stat_date
""" 

# 基于毛利版本的C端供需预估
gongxu_info_sql = """
select 
    distinct
    aa.city_id,
    aa.pred_date,
    bb.kuai_gongxu as dsr_k_cpt,
    bb.tehui_gongxu as dsr_th_cpt,
    bb.pred_cr_k,
    bb.pred_cr_th
from
    (
        select
            city_id,
            pred_date
        from
            df_sample
    ) aa
    inner join (
        select  
            a.city_id,
            a.pred_date,
            a.kuai_gongxu,
            a.tehui_gongxu,
            a.pred_cr as pred_cr_k,
            b.pred_cr as pred_cr_th
        from
            (
                select
                    city_id,
                    tdate as pred_date,
                    kuai_gongxu,
                    tehui_gongxu,
                    pred_cr
                from
                    -- bigdata_driver_ecosys_test.tmp_kuai_tehui_gongxu_cr_pred
                    bigdata_driver_ecosys_test.profit_kuai_tehui_gongxu_cr_pred
                where
                    dt = date_sub('{gmv_dt}',{gongxu_try_num})
                    and label = 'com_kuai_cr'
                    and model_version = 'v1'
            ) a
            left join (
                select
                    city_id,
                    tdate as pred_date,
                    kuai_gongxu,
                    tehui_gongxu,
                    pred_cr
                from
                    -- bigdata_driver_ecosys_test.tmp_kuai_tehui_gongxu_cr_pred
                    bigdata_driver_ecosys_test.profit_kuai_tehui_gongxu_cr_pred
                where
                    dt = date_sub('{gmv_dt}',{gongxu_try_num})
                    and label = 'com_tehui_cr'
                    and model_version = 'v1'
            ) b on a.city_id = b.city_id
            and a.pred_date = b.pred_date
            and a.kuai_gongxu = b.kuai_gongxu
            and a.tehui_gongxu = b.tehui_gongxu
    ) bb on aa.city_id = bb.city_id
    and aa.pred_date = bb.pred_date
"""

posroi_sql = '''
select
    city_id,group,
    pkhf,pkroi_c,pkroi_bp,
    thhf,throi_c,throi_bp,
    fkhf,fkroi_c,fkroi_bp
from
    bigdata_driver_ecosys_test.dape_profit_posroi_new
where
    dt = date_sub('{cur_day}', {roi_try_num})
'''

ctlrate_sql = '''
select
    city_id,
    pred_date,
    thhf_past,
    pkhf_past,
    gmv_pk,
    gmv_th,
    budget_th,
    budget_pk
from
    bigdata_driver_ecosys_test.ctl_tehui_pk_rate_budget_estimate
where
    dt = date_sub('{cur_day}', {rate_try_num})
'''

get_subsidy_rate_sql = """
select * from ppe.dwd_trip_mp_hufan_city_sr_di
    where simulation_version_name = 511
    and city_id = 1
"""

