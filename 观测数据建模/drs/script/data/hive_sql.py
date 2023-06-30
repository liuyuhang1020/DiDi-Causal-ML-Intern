# coding=utf-8
"""用于 pyspark 的 Hive SQL"""

# 获取指定品类的全量候选集
# TODO: 增加品类和城市参数
'''
    供需预测表：https://datamap.didichuxing.com/search?keyword=mp_data.app_trip_mkt_supply_demand_forecast_result_di
'''

pred_cr_sql = '''
select
    estimate_date as stat_date
    ,city_id
    ,cast(sum(if(key_id='objective_exp_openapi_pp', estimate_value, 0)) as Double) score -- 剔除openapi的客观cr
from 
    mp_data.app_trip_mkt_supply_demand_forecast_result_di
where
    dt = date_sub('{cr_dt}', {try_num}) -- 指定的 gmv dt 分区
    and  estimate_date between '{start_date}' and '{end_date}'
    and city_id in ({city_list})
    and product_name = '泛快2（快车&特惠快车&市内拼车）'
group by estimate_date, city_id
'''


pred_delta_gmv_sql = '''
select
    city_id,
    combo_idx,
    pred_delta_gmv
from
    bigdata_driver_ecosys_test.drs_pred_delta_gmv
where
    dt = '{dt}'
    and city_id in ({cities})
    and version = 'roi_iv_forest_v1'
) gmv
'''


pred_candidate_sql = '''
select
    a.dt dt,
    a.city_id city_id,
    a.combo_idx combo_idx,
    a.combo_subsidy_rate,
    is_candidate,
    nvl(pred_delta_gmv,0) delta_gmv ,
    nvl(pred_delta_profit,0) delta_profit,
    nvl(pred_roi_c,0) roi_c,
    nvl(pred_roi_bp,0) roi_bp,
    nvl(pred_cost,0) pred_cost,
    nvl(roi_var,0) roi_var
from(
    select
        dt
        ,city_id
        ,combo_idx
        ,combo_subsidy_rate
        ,is_candidate 
    from bigdata_driver_ecosys_test.drs_pas_candidate
    where dt = '{dt}'
    and version = '{version1}'
    and city_id in ({cities})
)a 
join(
    select 
        dt
        ,city_id
        ,combo_idx
        ,combo_subsidy_rate
        ,pred_delta_gmv
        ,pred_delta_profit
        ,pred_roi_c
        ,pred_roi_bp
        ,pred_cost
        ,roi_var
    from bigdata_driver_ecosys_test.drs_pred_delta_gmv
    where dt = '{dt}'
    and version = '{version2}'
    and city_id in ({cities})
)b 
on 
    a.city_id = b.city_id
    and a.combo_idx = b.combo_idx
'''


nor_zhuan_hufan_rate_sql = '''
 select  city_id
		,coalesce(sum(case when product_line == "zhuanche" then subsidy_hufan else 0 end) / sum(subsidy_hufan),0.3) as nor_zhuan_ratio 
from	bigdata_driver_ecosys_test.zhuan_cyd_index_for_zhuan_budget_allocation_v1
where   dt between date_sub('{cur_day}', 30) and '{cur_day}'
group by city_id
'''


gap_dignosis_sql = '''
select  result_date as stat_date
		,case 
            when tool = "智能盘古" then 'b'
            when tool = "加速卡" then "accelerate_card" 
        end as caller
		,city_id
		,fence_id
		,start_time
        ,cast(substring(start_time,1,2) as int) as start_hour
		,end_time
        ,case when substring(end_time,4,2) != '30' then cast(substring(end_time,1,2) as int)-1 else cast(substring(end_time,1,2) as int)  end as end_hour
		,cr
		,cr_bar
		,gmv_ratio
		,finish_count_ratio
		,call_count_ratio
		,call_cnt
		,gmv
        ,strategy_type
from	prod_smt_dw.smt_budget_diagnosis_result
where   dt = "{cr_dt}"
  and   product_id = "{product_id}"
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
    "b" as caller
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
        and product_name = "{product_name}"
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
        "accelerate_card" as caller
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
            and product_name = "{product_name}"
            --and product_name = '泛快含优含拼（快车（普通快车、A+、车大、巡游网约车）、市内拼车、优享、D1、特惠快车、涅槃）'
        group by
            city_id,
            estimate_date,
            floor(hour / 2)
       ) pred_df 
        on elestic_acc_card_df.city_id = pred_df.city_id
        and elestic_acc_card_df.start_dt = pred_df.stat_date
        and cast(elestic_acc_card_df.start_time as int) = cast(pred_df.hour as int)
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
            ccc.city_id,
            ccc.stat_date,
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
                        and product_name = "{product_name}"
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
                            and product_id = "{product_id}"
                            and version = "{version}"
                            and fence_id = -1
                            and result_date in ({st_end_times_str})
                    ) fknp_exp_cr on fkp_table.city_id = fknp_exp_cr.city_id
                    and fkp_table.stat_date = fknp_exp_cr.stat_date
                    and fkp_table.half_hour = fknp_exp_cr.half_hour
            )ccc
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
            and product_name = "{product_name}"
        group by
            city_id,
            estimate_date
    ) gmv_table on call_table.city_id = gmv_table.city_id
    and call_table.stat_date = gmv_table.stat_date
'''

asp_sql = '''
select
    city_id,
    avg(coalesce(gmv/finish_order_cnt,10.0)) as asp
from
    mp_data.dm_trip_mp_sd_core_1d
where
    dt between date_sub('{cur_day}',9) and date_sub('{cur_day}',2)
    and product_id = {product_id}
group by city_id
'''

zhuan_rsquare_sql = '''
select  city_id
		,pre_date as stat_date
        ,sum(case when key_id = "r2_roi_c" then value else 0 end) as r2_roi_c
        ,sum(case when key_id = "r2_roi_cb" then value else 0 end) as r2_roi_cb
from	bigdata_driver_ecosys_test.drs_daily_key_index_v1
where   dt = date_sub('{cur_day}',{try_num})
group by city_id,pre_date
'''


candidates_sql = '''
select 
        candidate_df.city_id
		,candidate_df.stat_date
		,combo_idx
        ,pk_hufan_rate
        ,th_hufan_rate
        ,zhuan_hufan_rate
        ,chuzu_hufan_rate
		,combo_hufan_rate
		,roi_c
		,roi_bp
        ,cast(dgmv_ratio as float)
		,(pk_hufan_rate * kuaiche_gmv + th_hufan_rate*tehui_gmv + zhuan_hufan_rate*zhuanche_gmv + chuzu_hufan_rate*chuzuche_gmv) as delta_cost
		,delta_bp
		,is_candidate
        ,cast(zhuanche_gmv as float)
        ,cast(kuaiche_gmv as float)
        ,cast(tehui_gmv as float)
        ,cast(chuzuche_gmv as float)
from 
(
select  city_id
		,pre_date as stat_date
		,combo_idx
        ,cast(nvl(split(combo_hufan_rate,',')[0],0) as float) as pk_hufan_rate
        ,cast(nvl(split(combo_hufan_rate,',')[1],0) as float) as th_hufan_rate
        ,cast(nvl(split(combo_hufan_rate,',')[2],0) as float) as zhuan_hufan_rate
        ,cast(nvl(split(combo_hufan_rate,',')[3],0) as float) as chuzu_hufan_rate
		,combo_hufan_rate
        ,dgmv_ratio
		,nvl(roi_c,0) as roi_c
		,nvl(roi_bp,0) as roi_bp
		,nvl(delta_bp,0) as delta_bp
		,is_candidate
from	bigdata_driver_ecosys_test.drs_pas_candidate_v2
where   dt = date_sub('{cur_day}',{try_num})
  and   version = '{version}'
) candidate_df
left join (
select  city_id
		,estimate_date as stat_date
        ,sum(case when key_id = "gmv" and product_name = "专车" then estimate_value else 0 end) as zhuanche_gmv
        ,sum(case when key_id = "gmv" and product_name = "快车" then estimate_value else 0 end) as kuaiche_gmv
        ,sum(case when key_id = "gmv" and product_name = "特惠自营" then estimate_value else 0 end) as tehui_gmv
        ,sum(case when key_id = "gmv" and product_name = "出租车" then estimate_value else 0 end) as chuzuche_gmv
from	mp_data.app_trip_mkt_supply_demand_forecast_result_di
where   dt = '{gmv_dt}'
group by city_id,estimate_date
)gmv_table on gmv_table.city_id = candidate_df.city_id
and gmv_table.stat_date = candidate_df.stat_date
'''

mps_candidate_sql = """
    select
        base.tdate stat_date,
        base.city_id city_id,
        base.combo_idx combo_idx,
        base.combo_subsidy_rate combo_subsidy_rate,
        is_candidate,
        cast(coalesce(pred_roi * pred_delta_cost,0) as Double) delta_gmv,
        cast(coalesce(pred_profit_roi * pred_delta_cost,0) as Double) delta_bp,
        cast(coalesce(pred_delta_cost,0) as Double) delta_cost,
        concat_ws(':',cast(base.city_id as  string),  base.combo_subsidy_rate) robot_k,
        cast(split(base.combo_subsidy_rate, ',')[0] as double) as kc_subsidy_rate,
        cast(split(base.combo_subsidy_rate, ',')[1] as double) as th_subsidy_rate,
        cast((pukuai_gmv*cast(split(base.combo_subsidy_rate, ',')[0] as double) + tehui_gmv * cast(split(base.combo_subsidy_rate, ',')[1] as double))/total_gmv as Double) crate,
        cast(total_gmv as DOUBLE) total_gmv, 
        cast(pukuai_gmv as DOUBLE) pukuai_gmv, 
        cast(tehui_gmv as DOUBLE) tehui_gmv
        from
        (select
            city_id,
            combo_idx,
            combo_subsidy_rate,
            is_candidate,
            tdate
        from
            smt_stg.budget_allocation_pas_candidate
        where
            concat_ws('-',year,month,day)='{activity_date}' and tdate between '{start_date}' and '{end_date}' 
            and city_id in ({city_list}) and version = 'v2'
        ) base
        left join
        (select *
         from
            (select
                city_id,
                combo_idx,
                combo_subsidy_rate,
                pred_roi,
                cast(concat_ws('-',year,month,day) as String) tdate
            from
                smt_stg.budget_allocation_pred_delta_gmv_v2
            where
                concat_ws('-',year,month,day)='{activity_date}' and city_id in ({city_list}) and version = 'roi_lgb_v1'
            )
            union
            (select
                city_id,
                combo_idx,
                combo_subsidy_rate,
                pred_roi,
                cast(date_add(concat_ws('-',year,month,day), 7) as String) tdate
            from
                smt_stg.budget_allocation_pred_delta_gmv_v2
            where
                concat_ws('-',year,month,day) between date_sub(date_add('{activity_date}', 1), 7) and date_sub('{end_date}', 7) 
                and city_id in ({city_list}) and version = 'roi_lgb_v1'
            )        
        ) gmv
        on base.city_id = gmv.city_id and base.combo_idx = gmv.combo_idx and base.combo_subsidy_rate=gmv.combo_subsidy_rate and base.tdate = gmv.tdate
        left join
        (select *
        from
            (select
                city_id,
                combo_idx,
                combo_subsidy_rate,
                pred_profit_roi,
                cast(concat_ws('-',year,month,day) as String) tdate
            from
                smt_stg.budget_allocation_pred_delta_profit_ivf
            where
                concat_ws('-',year,month,day)='{activity_date}' and city_id in ({city_list}) and version = 'iv_forest_v1'
            )
            union
            (select
                city_id,
                combo_idx,
                combo_subsidy_rate,
                pred_profit_roi,
                cast(date_add(concat_ws('-',year,month,day), 7)  as String) tdate
            from smt_stg.budget_allocation_pred_delta_profit_ivf
            where
                concat_ws('-',year,month,day) between date_sub(date_add('{activity_date}', 1), 7) and date_sub('{end_date}', 7) 
                and city_id in ({city_list}) and version = 'iv_forest_v1'
            )
        ) profit
        on base.city_id = profit.city_id and base.combo_idx = profit.combo_idx and base.combo_subsidy_rate = profit.combo_subsidy_rate and base.tdate = profit.tdate
        left join
        (select
            city_id,
            combo_idx,
            combo_subsidy_rate,
            pred_delta_cost,
            tdate,
            pukuai_gmv,
            tehui_gmv
        from
            smt_stg.budget_allocation_pred_delta_cost
        where
            concat_ws('-',year,month,day)='{activity_date}' and tdate between '{start_date}' and '{end_date}'   
            and city_id in ({city_list}) and version = 'v1'
        ) cost
        on base.city_id = cost.city_id and base.combo_idx = cost.combo_idx and base.combo_subsidy_rate=cost.combo_subsidy_rate and base.tdate = cost.tdate
        left join
        ( select
            estimate_date as pred_date,
            city_id,
            sum(case when key_id = 'gmv' then estimate_value else 0.0 end ) as total_gmv,
            coalesce( sum( case when key_id = 'gmv' then estimate_value else 0.0 end ) / sum( case when key_id = 'finish_order_cnt' then estimate_value else 0.0 end ),10.0) as asp,
            sum(case when key_id = 'total_tsh' then estimate_value else 0.0 end)/sum( case when key_id = 'compete_call_cnt' then estimate_value else 0.0 end ) as dsr
        from
            mp_data.app_trip_mkt_supply_demand_forecast_result_di
        where
            dt = '{gmv_dt}' -- 指定的 gmv dt 分区
            and estimate_date between '{start_date}' and '{end_date}'
            and product_name = '泛快含优不含拼（快车（普通快车、A+、车大、巡游网约车）、优享、D1、特惠快车、涅槃）' --新泛快
        group by
            estimate_date,
            city_id) dsr
     on base.city_id = dsr.city_id and base.tdate= dsr.pred_date
    """


wyc_ecr_sql = """
select
    city_id
    ,cast(date_add(dt, 7) as String) stat_date
    ,bubble_cnt
    ,compete_call_cnt
    ,cast(nvl(compete_call_cnt/bubble_cnt, 0) as Double) wyc_ecr
from 
    smt_stg.budget_allocation_xfk_city_daily_table
where dt between date_sub('{start_date}', 7) and date_sub('{end_date}', 7) and city_id in ({city_list})
    and product_line='网约车'
    and group_name='rgroup_joint_blank'
"""