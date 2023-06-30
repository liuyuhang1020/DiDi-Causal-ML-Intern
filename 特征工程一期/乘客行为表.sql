select
    city_id,
    sum(bubble_cnt) as bubble_cnt,
    --冒泡数
    sum(if(pas_active_tag = 2, bubble_cnt, 0)) as high_freq_bubble_cnt,
    --中高活跃用户冒泡数
    sum(if(pas_active_tag = 1, bubble_cnt, 0)) as low_freq_bubble_cnt,
    --低活跃用户冒泡数
    sum(if(pas_active_tag = 0, bubble_cnt, 0)) as xcl_freq_bubble_cnt,
    --新沉流用户冒泡数
    sum(call_cnt) as call_cnt,
    --发单数
    sum(if(pas_active_tag = 2, call_cnt, 0)) as high_freq_call_cnt,
    --中高活跃用户发单数
    sum(if(pas_active_tag = 1, call_cnt, 0)) as low_freq_call_cnt,
    --低活跃用户发单数
    sum(if(pas_active_tag = 0, call_cnt, 0)) as xcl_freq_call_cnt,
    --新沉流用户发单数
    sum(success_cnt) as success_cnt,
    --完单数
    sum(if(pas_active_tag = 2, success_cnt, 0)) as high_freq_success_cnt,
    --中高活跃用户完单数
    sum(if(pas_active_tag = 1, success_cnt, 0)) as low_freq_success_cnt,
    --低活跃用户完单数
    sum(if(pas_active_tag = 0, success_cnt, 0)) as xcl_freq_success_cnt,
    --新沉流用户完单数
    sum(gmv) as gmv,
    --完单GMV
    sum(if(pas_active_tag = 2, gmv, 0)) as high_freq_gmv,
    --中高活跃用户完单GMV
    sum(if(pas_active_tag = 1, gmv, 0)) as low_freq_gmv,
    --低活跃用户完单GMV
    sum(if(pas_active_tag = 0, gmv, 0)) as xcl_freq_gmv,
    --新沉流用户完单GMV
    sum(driver_divide) as driver_divide,
    --司机抽成
    sum(business_profit) as business_profit,
    --毛利额
    sum(subsidy_hufan) as subsidy_hufan,
    --完单呼返
    sum(subsidy_c) as subsidy_c,
    --完单C补
    sum(subsidy_b) as subsidy_b, --完单B补
    dt,
    product_name
from
    (
        select
            passenger_id,
            dt,
            city_id,
            case
                when product_line = '特惠自营' then '特惠快车'
                when product_line = '普通快车非拼' then '普通快车'
                else product_line
            end as product_name,
            case
                when product_line = '网+出' then '网+出'
                when product_line = '出租车' then '网+出'
                else '网约车'
            end as pas_product_name,
            sum(bubble_cnt) as bubble_cnt,
            --冒泡数
            sum(objective_call_cnt) as call_cnt,
            --发单数
            sum(suc_cnt) as success_cnt,
            --完单数
            sum(gmv) as gmv,
            --完单GMV
            sum(online_driver_divide) as driver_divide,
            --司机抽成
            sum(business_profit) as business_profit,
            --毛利额
            sum(subsidy_hufan) as subsidy_hufan,
            --完单呼返
            sum(subsidy_c) as subsidy_c,
            --完单C补
            sum(subsidy_b) as subsidy_b --完单B补
        from
            mp_data.dwm_trip_trd_psg_core_di
        where
            dt = '${BIZ_DATE_LINE}'
            and product_line in (
                '特惠自营',
                '普通快车非拼',
                '市内拼车',
                '专车',
                '出租车',
                '网约车',
                '网+出',
                '新泛快',
                '泛快大盘'
            )
        group by
            passenger_id,
            dt,
            city_id,
            product_line
    ) a
    left join (
        select
            passenger_id,
            product_line as pas_product_name,
            case
                when pas_call_active_tag in (3, 4) then 2 --中高活跃用户
                when pas_call_active_tag = 2 then 1 --低活跃用户
                else 0 --新沉流用户
            end as pas_active_tag
        from
            dd_cdm.dm_trip_pas_label_active_df
        where
            dt = '${BIZ_DATE_LINE}'
            and product_line in ('网约车', '网+出')
    ) b on a.passenger_id = b.passenger_id
    and a.pas_product_name = b.pas_product_name
group by
    dt,
    city_id,
    product_name;