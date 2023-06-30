#!/usr/bin/env python
# -*- encoding: utf-8 -*-

taocan_rate = '''
    select
        b_rate,
        estimate,
        start_dt,
        city_id,
        target_name
    from 
    gulfstream.sinan_online_predict_public_function_estimate
    where
    	dt = date_sub('{cur_day}', {pred_ela_num}+2)
        and incentive_type='city_daily_dgmvrate_fankuai_package_30'
        and model_version='v2.3.1'
        and start_dt = '{start_date}'
        and city_id in ({city_list})

'''


taocan_up_low ='''
    select
        dt,
        city_id,
        minimal_net_subsidy,
        maximal_net_subsidy
    from 
        gulfstream.mps_package_city_net_cost_range_d
    where 
        dt = date_sub('{cur_day}', {pred_bound_num}+2)
        and city_id in ({city_list})

'''


taocan_df = '''
    select
        city_id,
        value,
        stat_date as pred_date
    from
        riemann.daily_predict_w
    where
        metric = 'total_gmv' 
        and concat_ws('-',year,month,day) = date_sub('{cur_day}',{pred_try_num}+2)
        and stat_date in ('{pred_dates}') 
        and product = 'wyc' 
        and city_id in ({city_list})
'''


