with passenger_table as (
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
        sum(subsidy_b) as subsidy_b,
        --完单B补
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
        product_name
),
t1 as (
    select
        city_id,
        sum(total_tsh) as total_tsh,
        sum(th_yj_tsh) as th_yj_tsh,
        sum(answer_order_cnt) as answer_order_cnt,
        sum(charge_dur) as charge_dur,
        dt,
        case
            when product_name = '特惠自营' then '特惠快车'
            when product_name = '泛快含优含拼（快车（普通快车、A+、车大、巡游网约车）、市内拼车、优享、D1、特惠快车、涅槃）' then '泛快大盘'
            else product_name
        end as product_name
    from
        mp_data.dm_trip_mp_sd_core_1d
    where
        dt = '${BIZ_DATE_LINE}'
        and product_name in (
            '特惠自营',
            '快车',
            '普通快车',
            '市内拼车',
            '专车',
            '出租车',
            '网约车',
            '网+出',
            '泛快含优含拼（快车（普通快车、A+、车大、巡游网约车）、市内拼车、优享、D1、特惠快车、涅槃）'
        )
    group by
        dt,
        city_id,
        product_name
),
city_table as (
    select
        aa.city_id as city_id,
        total_tsh,
        --TSH（没有普通快车、市内拼车，使用快车替代普通快车）
        th_yj_tsh,
        --愿接特惠时长
        answer_order_cnt,
        --应答数
        charge_dur,
        --计费时长（没有普通快车、市内拼车、特惠快车，使用快车替代普通快车和特惠快车）
        --TPH = 完单数 / TSH
        --供需比 = TSH / 发单数
        --需供比 = 发单数 / TSH
        --应答率 = 应答数 / 发单数
        aa.dt as dt,
        aa.product_name as product_name
    from
        (
            select
                a.dt as dt,
                a.city_id as city_id,
                a.product_name as product_name,
                total_tsh,
                th_yj_tsh,
                answer_order_cnt,
                case
                    when a.product_name in ('特惠快车', '普通快车') then '快车'
                    else a.product_name
                end as src_product_name
            from
                (
                    select
                        dt,
                        city_id,
                        product_name,
                        th_yj_tsh,
                        answer_order_cnt
                    from
                        t1
                    where
                        not product_name = '快车'
                ) a
                left join (
                    select
                        dt,
                        city_id,
                        case
                            when product_name = '快车' then '普通快车'
                            else product_name
                        end as product_name,
                        total_tsh
                    from
                        t1
                    where
                        not product_name = '普通快车'
                ) b on a.dt = b.dt
                and a.city_id = b.city_id
                and a.product_name = b.product_name
        ) aa
        left join (
            select
                dt,
                city_id,
                product_name,
                charge_dur
            from
                t1
        ) bb on aa.dt = bb.dt
        and aa.city_id = bb.city_id
        and aa.src_product_name = bb.product_name
),
t2 as (
    select
        aa.city_id as city_id,
        morning_peak_bubble_cnt,
        evening_peak_bubble_cnt,
        night_peak_bubble_cnt,
        morning_peak_call_cnt,
        evening_peak_call_cnt,
        night_peak_call_cnt,
        lineup_call_cnt,
        morning_peak_lineup_call_cnt,
        evening_peak_lineup_call_cnt,
        night_peak_lineup_call_cnt,
        est_dur,
        est_arrive_dur,
        morning_peak_est_arrive_dur,
        evening_peak_est_arrive_dur,
        night_peak_est_arrive_dur,
        est_arrive_dis,
        morning_peak_success_cnt,
        evening_peak_success_cnt,
        night_peak_success_cnt,
        morning_peak_gmv,
        evening_peak_gmv,
        night_peak_gmv,
        long_dis_success_cnt,
        short_dis_success_cnt,
        long_dis_gmv,
        short_dis_gmv,
        aa.dt as dt,
        aa.level_1_product as level_1_product,
        aa.level_2_product as level_2_product,
        aa.level_3_product as level_3_product
    from
        (
            select
                a.city_id as city_id,
                morning_peak_bubble_cnt,
                evening_peak_bubble_cnt,
                night_peak_bubble_cnt,
                morning_peak_call_cnt,
                evening_peak_call_cnt,
                night_peak_call_cnt,
                lineup_call_cnt,
                morning_peak_lineup_call_cnt,
                evening_peak_lineup_call_cnt,
                night_peak_lineup_call_cnt,
                est_dur,
                est_arrive_dur,
                morning_peak_est_arrive_dur,
                evening_peak_est_arrive_dur,
                night_peak_est_arrive_dur,
                est_arrive_dis,
                a.dt as dt,
                a.level_1_product as level_1_product,
                a.level_2_product as level_2_product,
                a.level_3_product as level_3_product
            from
                (
                    select
                        city_id,
                        count(
                            if(
                                bubble_hour >= 07
                                and bubble_hour < 10,
                                bubble_hour,
                                null
                            )
                        ) as morning_peak_bubble_cnt,
                        --早高峰冒泡数
                        count(
                            if(
                                bubble_hour >= 17
                                and bubble_hour < 20,
                                bubble_hour,
                                null
                            )
                        ) as evening_peak_bubble_cnt,
                        --晚高峰冒泡数
                        count(
                            if(
                                bubble_hour >= 21
                                or bubble_hour < 5,
                                bubble_hour,
                                null
                            )
                        ) as night_peak_bubble_cnt,
                        --夜高峰冒泡数
                        dt,
                        level_1_product,
                        level_2_product,
                        level_3_product
                    from
                        (
                            select
                                dt,
                                bubble_id,
                                start_city_id as city_id,
                                substring(bubble_time, 12, 2) as bubble_hour
                            from
                                dd_cdm.dwd_trd_bubble_distinct_di
                            where
                                dt = '${BIZ_DATE_LINE}'
                                and level1_product_id in (110000, 120000)
                        ) a
                        left join(
                            select
                                distinct bubble_id,
                                level_1_product,
                                level_2_product,
                                level_3_product
                            from
                                dd_cdm.dwd_trip_trd_bubble_anycar_di
                            where
                                dt = '${BIZ_DATE_LINE}'
                                and level_1_product in (110000, 120000)
                        ) b on a.bubble_id = b.bubble_id
                    group by
                        dt,
                        city_id,
                        level_1_product,
                        level_2_product,
                        level_3_product
                ) a full
                join (
                    select
                        city_id,
                        count(
                            if(
                                call_hour >= 07
                                and call_hour < 10,
                                call_hour,
                                null
                            )
                        ) as morning_peak_call_cnt,
                        --早高峰发单数
                        count(
                            if(
                                call_hour >= 17
                                and call_hour < 20,
                                call_hour,
                                null
                            )
                        ) as evening_peak_call_cnt,
                        --晚高峰发单数
                        count(
                            if(
                                call_hour >= 21
                                or call_hour < 5,
                                call_hour,
                                null
                            )
                        ) as night_peak_call_cnt,
                        --夜高峰发单数
                        sum(is_lineup) as lineup_call_cnt,
                        --排队订单数
                        sum(
                            if(
                                call_hour >= 07
                                and call_hour < 10,
                                is_lineup,
                                0
                            )
                        ) as morning_peak_lineup_call_cnt,
                        --早高峰排队订单数
                        sum(
                            if(
                                call_hour >= 17
                                and call_hour < 20,
                                is_lineup,
                                0
                            )
                        ) as evening_peak_lineup_call_cnt,
                        --晚高峰排队订单数
                        sum(
                            if(
                                call_hour >= 21
                                or call_hour < 5,
                                is_lineup,
                                0
                            )
                        ) as night_peak_lineup_call_cnt,
                        --夜高峰排队订单数
                        sum(est_dur) as est_dur,
                        --预估时长
                        sum(est_arrive_dur) as est_arrive_dur,
                        --预计接驾时长
                        sum(
                            if(
                                call_hour >= 07
                                and call_hour < 10,
                                est_arrive_dur,
                                0
                            )
                        ) as morning_peak_est_arrive_dur,
                        --早高峰预计接驾时长
                        sum(
                            if(
                                call_hour >= 17
                                and call_hour < 20,
                                est_arrive_dur,
                                0
                            )
                        ) as evening_peak_est_arrive_dur,
                        --晚高峰预计接驾时长
                        sum(
                            if(
                                call_hour >= 21
                                or call_hour < 5,
                                est_arrive_dur,
                                0
                            )
                        ) as night_peak_est_arrive_dur,
                        --夜高峰预计接驾时长
                        sum(est_arrive_dis) as est_arrive_dis,
                        --预计接驾距离
                        dt,
                        level_1_product,
                        level_2_product,
                        level_3_product
                    from
                        (
                            select
                                dt,
                                call_city as city_id,
                                level_1_product,
                                level_2_product,
                                level_3_product,
                                substring(call_time, 12, 2) as call_hour,
                                is_lineup,
                                est_dur,
                                est_arrive_dur,
                                est_arrive_dis
                            from
                                decdm.dwd_gflt_ord_order_base_di
                            where
                                dt = '${BIZ_DATE_LINE}'
                                and level_1_product in (110000, 120000)
                        ) t
                    group by
                        dt,
                        city_id,
                        level_1_product,
                        level_2_product,
                        level_3_product
                ) b on a.dt = b.dt
                and a.city_id = b.city_id
                and a.level_1_product = b.level_1_product
                and a.level_2_product = b.level_2_product
                and a.level_3_product = b.level_3_product
        ) aa full
        join (
            select
                city_id,
                count(
                    if(
                        end_charge_hour >= 07
                        and end_charge_hour < 10,
                        end_charge_hour,
                        null
                    )
                ) as morning_peak_success_cnt,
                --早高峰完单数
                count(
                    if(
                        end_charge_hour >= 17
                        and end_charge_hour < 20,
                        end_charge_hour,
                        null
                    )
                ) as evening_peak_success_cnt,
                --晚高峰完单数
                count(
                    if(
                        end_charge_hour >= 21
                        or end_charge_hour < 5,
                        end_charge_hour,
                        null
                    )
                ) as night_peak_success_cnt,
                --夜高峰完单数
                sum(
                    if(
                        end_charge_hour >= 07
                        and end_charge_hour < 10,
                        gmv_amt / 100,
                        0
                    )
                ) as morning_peak_gmv,
                --早高峰完单GMV
                sum(
                    if(
                        end_charge_hour >= 17
                        and end_charge_hour < 20,
                        gmv_amt,
                        0
                    )
                ) as evening_peak_gmv,
                --晚高峰完单GMV
                sum(
                    if(
                        end_charge_hour >= 21
                        or end_charge_hour < 5,
                        gmv_amt,
                        0
                    )
                ) as night_peak_gmv,
                --夜高峰完单GMV
                count(
                    if(
                        est_dis >= 15000,
                        est_dis,
                        null
                    )
                ) as long_dis_success_cnt,
                --长距离完单数
                count(
                    if(
                        est_dis < 15000,
                        est_dis,
                        null
                    )
                ) as short_dis_success_cnt,
                --短距离完单数
                sum(
                    if(
                        est_dis >= 15000,
                        gmv_amt / 100,
                        0
                    )
                ) as long_dis_gmv,
                --长距离完单GMV
                sum(
                    if(
                        est_dis < 15000,
                        gmv_amt / 100,
                        0
                    )
                ) as short_dis_gmv,
                --短距离完单GMV
                dt,
                level_1_product,
                level_2_product,
                level_3_product
            from
                (
                    select
                        dt,
                        city_id,
                        level_1_product,
                        level_2_product,
                        level_3_product,
                        substring(end_charge_time, 12, 2) as end_charge_hour,
                        est_dis,
                        gmv_amt
                    from
                        dd_cdm.dwd_trip_trd_attribute_call_order_base_di
                    where
                        dt = '${BIZ_DATE_LINE}'
                        and level_1_product in (110000, 120000)
                ) t
            group by
                dt,
                city_id,
                level_1_product,
                level_2_product,
                level_3_product
        ) bb on aa.dt = bb.dt
        and aa.city_id = bb.city_id
        and aa.level_1_product = bb.level_1_product
        and aa.level_2_product = bb.level_2_product
        and aa.level_3_product = bb.level_3_product
),
order_table as (
    select
        city_id,
        dt,
        case
            when level_3_product = 110103 then '特惠快车'
            when level_3_product = 110101 then '普通快车'
        end as product_name,
        sum(morning_peak_bubble_cnt) as morning_peak_bubble_cnt,
        sum(evening_peak_bubble_cnt) as evening_peak_bubble_cnt,
        sum(night_peak_bubble_cnt) as night_peak_bubble_cnt,
        sum(morning_peak_call_cnt) as morning_peak_call_cnt,
        sum(evening_peak_call_cnt) as evening_peak_call_cnt,
        sum(night_peak_call_cnt) as night_peak_call_cnt,
        sum(lineup_call_cnt) as lineup_call_cnt,
        sum(morning_peak_lineup_call_cnt) as morning_peak_lineup_call_cnt,
        sum(evening_peak_lineup_call_cnt) as evening_peak_lineup_call_cnt,
        sum(night_peak_lineup_call_cnt) as night_peak_lineup_call_cnt,
        sum(est_dur) as est_dur,
        sum(est_arrive_dur) as est_arrive_dur,
        sum(morning_peak_est_arrive_dur) as morning_peak_est_arrive_dur,
        sum(evening_peak_est_arrive_dur) as evening_peak_est_arrive_dur,
        sum(night_peak_est_arrive_dur) as night_peak_est_arrive_dur,
        sum(est_arrive_dis) as est_arrive_dis,
        sum(morning_peak_success_cnt) as morning_peak_success_cnt,
        sum(evening_peak_success_cnt) as evening_peak_success_cnt,
        sum(night_peak_success_cnt) as night_peak_success_cnt,
        sum(morning_peak_gmv) as morning_peak_gmv,
        sum(evening_peak_gmv) as evening_peak_gmv,
        sum(night_peak_gmv) as night_peak_gmv,
        sum(long_dis_success_cnt) as long_dis_success_cnt,
        sum(short_dis_success_cnt) as short_dis_success_cnt,
        sum(long_dis_gmv) as long_dis_gmv,
        sum(short_dis_gmv) as short_dis_gmv
    from
        t2
    where
        level_3_product in (110103, 110101)
    group by
        city_id,
        dt,
        case
            when level_3_product = 110103 then '特惠快车'
            when level_3_product = 110101 then '普通快车'
        end
    union
    select
        city_id,
        dt,
        case
            when level_2_product = 110200 then '市内拼车'
            when level_2_product = 110500 then '专车'
        end as product_name,
        sum(morning_peak_bubble_cnt) as morning_peak_bubble_cnt,
        sum(evening_peak_bubble_cnt) as evening_peak_bubble_cnt,
        sum(night_peak_bubble_cnt) as night_peak_bubble_cnt,
        sum(morning_peak_call_cnt) as morning_peak_call_cnt,
        sum(evening_peak_call_cnt) as evening_peak_call_cnt,
        sum(night_peak_call_cnt) as night_peak_call_cnt,
        sum(lineup_call_cnt) as lineup_call_cnt,
        sum(morning_peak_lineup_call_cnt) as morning_peak_lineup_call_cnt,
        sum(evening_peak_lineup_call_cnt) as evening_peak_lineup_call_cnt,
        sum(night_peak_lineup_call_cnt) as night_peak_lineup_call_cnt,
        sum(est_dur) as est_dur,
        sum(est_arrive_dur) as est_arrive_dur,
        sum(morning_peak_est_arrive_dur) as morning_peak_est_arrive_dur,
        sum(evening_peak_est_arrive_dur) as evening_peak_est_arrive_dur,
        sum(night_peak_est_arrive_dur) as night_peak_est_arrive_dur,
        sum(est_arrive_dis) as est_arrive_dis,
        sum(morning_peak_success_cnt) as morning_peak_success_cnt,
        sum(evening_peak_success_cnt) as evening_peak_success_cnt,
        sum(night_peak_success_cnt) as night_peak_success_cnt,
        sum(morning_peak_gmv) as morning_peak_gmv,
        sum(evening_peak_gmv) as evening_peak_gmv,
        sum(night_peak_gmv) as night_peak_gmv,
        sum(long_dis_success_cnt) as long_dis_success_cnt,
        sum(short_dis_success_cnt) as short_dis_success_cnt,
        sum(long_dis_gmv) as long_dis_gmv,
        sum(short_dis_gmv) as short_dis_gmv
    from
        t2
    where
        level_2_product in (110200, 110500)
    group by
        city_id,
        dt,
        case
            when level_2_product = 110200 then '市内拼车'
            when level_2_product = 110500 then '专车'
        end
    union
    select
        city_id,
        dt,
        case
            when level_1_product = 120000 then '出租车'
            when level_1_product = 110000 then '网约车'
        end as product_name,
        sum(morning_peak_bubble_cnt) as morning_peak_bubble_cnt,
        sum(evening_peak_bubble_cnt) as evening_peak_bubble_cnt,
        sum(night_peak_bubble_cnt) as night_peak_bubble_cnt,
        sum(morning_peak_call_cnt) as morning_peak_call_cnt,
        sum(evening_peak_call_cnt) as evening_peak_call_cnt,
        sum(night_peak_call_cnt) as night_peak_call_cnt,
        sum(lineup_call_cnt) as lineup_call_cnt,
        sum(morning_peak_lineup_call_cnt) as morning_peak_lineup_call_cnt,
        sum(evening_peak_lineup_call_cnt) as evening_peak_lineup_call_cnt,
        sum(night_peak_lineup_call_cnt) as night_peak_lineup_call_cnt,
        sum(est_dur) as est_dur,
        sum(est_arrive_dur) as est_arrive_dur,
        sum(morning_peak_est_arrive_dur) as morning_peak_est_arrive_dur,
        sum(evening_peak_est_arrive_dur) as evening_peak_est_arrive_dur,
        sum(night_peak_est_arrive_dur) as night_peak_est_arrive_dur,
        sum(est_arrive_dis) as est_arrive_dis,
        sum(morning_peak_success_cnt) as morning_peak_success_cnt,
        sum(evening_peak_success_cnt) as evening_peak_success_cnt,
        sum(night_peak_success_cnt) as night_peak_success_cnt,
        sum(morning_peak_gmv) as morning_peak_gmv,
        sum(evening_peak_gmv) as evening_peak_gmv,
        sum(night_peak_gmv) as night_peak_gmv,
        sum(long_dis_success_cnt) as long_dis_success_cnt,
        sum(short_dis_success_cnt) as short_dis_success_cnt,
        sum(long_dis_gmv) as long_dis_gmv,
        sum(short_dis_gmv) as short_dis_gmv
    from
        t2
    where
        level_1_product in (120000, 110000)
    group by
        city_id,
        dt,
        case
            when level_1_product = 120000 then '出租车'
            when level_1_product = 110000 then '网约车'
        end
    union
    select
        city_id,
        dt,
        '网+出' product_name,
        sum(morning_peak_bubble_cnt) as morning_peak_bubble_cnt,
        sum(evening_peak_bubble_cnt) as evening_peak_bubble_cnt,
        sum(night_peak_bubble_cnt) as night_peak_bubble_cnt,
        sum(morning_peak_call_cnt) as morning_peak_call_cnt,
        sum(evening_peak_call_cnt) as evening_peak_call_cnt,
        sum(night_peak_call_cnt) as night_peak_call_cnt,
        sum(lineup_call_cnt) as lineup_call_cnt,
        sum(morning_peak_lineup_call_cnt) as morning_peak_lineup_call_cnt,
        sum(evening_peak_lineup_call_cnt) as evening_peak_lineup_call_cnt,
        sum(night_peak_lineup_call_cnt) as night_peak_lineup_call_cnt,
        sum(est_dur) as est_dur,
        sum(est_arrive_dur) as est_arrive_dur,
        sum(morning_peak_est_arrive_dur) as morning_peak_est_arrive_dur,
        sum(evening_peak_est_arrive_dur) as evening_peak_est_arrive_dur,
        sum(night_peak_est_arrive_dur) as night_peak_est_arrive_dur,
        sum(est_arrive_dis) as est_arrive_dis,
        sum(morning_peak_success_cnt) as morning_peak_success_cnt,
        sum(evening_peak_success_cnt) as evening_peak_success_cnt,
        sum(night_peak_success_cnt) as night_peak_success_cnt,
        sum(morning_peak_gmv) as morning_peak_gmv,
        sum(evening_peak_gmv) as evening_peak_gmv,
        sum(night_peak_gmv) as night_peak_gmv,
        sum(long_dis_success_cnt) as long_dis_success_cnt,
        sum(short_dis_success_cnt) as short_dis_success_cnt,
        sum(long_dis_gmv) as long_dis_gmv,
        sum(short_dis_gmv) as short_dis_gmv
    from
        t2
    where
        level_1_product in (120000, 110000)
    group by
        city_id,
        dt
    union
    select
        city_id,
        dt,
        '新泛快' as product_name,
        sum(morning_peak_bubble_cnt) as morning_peak_bubble_cnt,
        sum(evening_peak_bubble_cnt) as evening_peak_bubble_cnt,
        sum(night_peak_bubble_cnt) as night_peak_bubble_cnt,
        sum(morning_peak_call_cnt) as morning_peak_call_cnt,
        sum(evening_peak_call_cnt) as evening_peak_call_cnt,
        sum(night_peak_call_cnt) as night_peak_call_cnt,
        sum(lineup_call_cnt) as lineup_call_cnt,
        sum(morning_peak_lineup_call_cnt) as morning_peak_lineup_call_cnt,
        sum(evening_peak_lineup_call_cnt) as evening_peak_lineup_call_cnt,
        sum(night_peak_lineup_call_cnt) as night_peak_lineup_call_cnt,
        sum(est_dur) as est_dur,
        sum(est_arrive_dur) as est_arrive_dur,
        sum(morning_peak_est_arrive_dur) as morning_peak_est_arrive_dur,
        sum(evening_peak_est_arrive_dur) as evening_peak_est_arrive_dur,
        sum(night_peak_est_arrive_dur) as night_peak_est_arrive_dur,
        sum(est_arrive_dis) as est_arrive_dis,
        sum(morning_peak_success_cnt) as morning_peak_success_cnt,
        sum(evening_peak_success_cnt) as evening_peak_success_cnt,
        sum(night_peak_success_cnt) as night_peak_success_cnt,
        sum(morning_peak_gmv) as morning_peak_gmv,
        sum(evening_peak_gmv) as evening_peak_gmv,
        sum(night_peak_gmv) as night_peak_gmv,
        sum(long_dis_success_cnt) as long_dis_success_cnt,
        sum(short_dis_success_cnt) as short_dis_success_cnt,
        sum(long_dis_gmv) as long_dis_gmv,
        sum(short_dis_gmv) as short_dis_gmv
    from
        t2
    where
        level_3_product in (110103, 110101, 110102)
    group by
        city_id,
        dt
    union
    select
        city_id,
        dt,
        '泛快大盘' as product_name,
        sum(morning_peak_bubble_cnt) as morning_peak_bubble_cnt,
        sum(evening_peak_bubble_cnt) as evening_peak_bubble_cnt,
        sum(night_peak_bubble_cnt) as night_peak_bubble_cnt,
        sum(morning_peak_call_cnt) as morning_peak_call_cnt,
        sum(evening_peak_call_cnt) as evening_peak_call_cnt,
        sum(night_peak_call_cnt) as night_peak_call_cnt,
        sum(lineup_call_cnt) as lineup_call_cnt,
        sum(morning_peak_lineup_call_cnt) as morning_peak_lineup_call_cnt,
        sum(evening_peak_lineup_call_cnt) as evening_peak_lineup_call_cnt,
        sum(night_peak_lineup_call_cnt) as night_peak_lineup_call_cnt,
        sum(est_dur) as est_dur,
        sum(est_arrive_dur) as est_arrive_dur,
        sum(morning_peak_est_arrive_dur) as morning_peak_est_arrive_dur,
        sum(evening_peak_est_arrive_dur) as evening_peak_est_arrive_dur,
        sum(night_peak_est_arrive_dur) as night_peak_est_arrive_dur,
        sum(est_arrive_dis) as est_arrive_dis,
        sum(morning_peak_success_cnt) as morning_peak_success_cnt,
        sum(evening_peak_success_cnt) as evening_peak_success_cnt,
        sum(night_peak_success_cnt) as night_peak_success_cnt,
        sum(morning_peak_gmv) as morning_peak_gmv,
        sum(evening_peak_gmv) as evening_peak_gmv,
        sum(night_peak_gmv) as night_peak_gmv,
        sum(long_dis_success_cnt) as long_dis_success_cnt,
        sum(short_dis_success_cnt) as short_dis_success_cnt,
        sum(long_dis_gmv) as long_dis_gmv,
        sum(short_dis_gmv) as short_dis_gmv
    from
        t2
    where
        level_2_product in (110100, 110200, 110400, 110800, 110900)
    group by
        city_id,
        dt
),
t3 as(
    select
        area_id as city_id,
        count(if(exp_discount < 70, exp_discount, null)) as discount_00_to_70_cnt,
        count(
            if(
                exp_discount >= 70
                and exp_discount < 80,
                exp_discount,
                null
            )
        ) as discount_70_to_80_cnt,
        count(
            if(
                exp_discount >= 80
                and exp_discount < 90,
                exp_discount,
                null
            )
        ) as discount_80_to_90_cnt,
        count(
            if(
                exp_discount >= 90
                and exp_discount < 95,
                exp_discount,
                null
            )
        ) as discount_90_to_95_cnt,
        count(
            if(
                exp_discount >= 95
                and exp_discount <= 100,
                exp_discount,
                null
            )
        ) as discount_95_to_100_cnt,
        dt,
        current_product_type
    from
        mp_data.dwd_trip_mp_hufan_bubble_di
    where
        dt = '${BIZ_DATE_LINE}'
        and current_product_type in (
            206,
            2061,
            210,
            208,
            315,
            209,
            100,
            400,
            900,
            314,
            2200,
            500
        )
    group by
        dt,
        area_id,
        current_product_type
),
discount_table as (
    select
        city_id,
        dt,
        case
            when current_product_type = 314 then '特惠快车'
            when current_product_type = 206 then '普通快车'
            when current_product_type in (100, 400) then '专车'
            when current_product_type = 500 then '出租车'
        end as product_name,
        sum(discount_00_to_70_cnt) as discount_00_to_70_cnt,
        sum(discount_70_to_80_cnt) as discount_70_to_80_cnt,
        sum(discount_80_to_90_cnt) as discount_80_to_90_cnt,
        sum(discount_90_to_95_cnt) as discount_90_to_95_cnt,
        sum(discount_95_to_100_cnt) as discount_95_to_100_cnt
    from
        t3
    where
        current_product_type in (314, 206, 100, 400, 500)
    group by
        dt,
        city_id,
        case
            when current_product_type = 314 then '特惠快车'
            when current_product_type = 206 then '普通快车'
            when current_product_type in (100, 400) then '专车'
            when current_product_type = 500 then '出租车'
        end
    union
    select
        city_id,
        dt,
        '网约车' product_name,
        sum(discount_00_to_70_cnt) as discount_00_to_70_cnt,
        sum(discount_70_to_80_cnt) as discount_70_to_80_cnt,
        sum(discount_80_to_90_cnt) as discount_80_to_90_cnt,
        sum(discount_90_to_95_cnt) as discount_90_to_95_cnt,
        sum(discount_95_to_100_cnt) as discount_95_to_100_cnt
    from
        t3
    where
        current_product_type in (
            206,
            2061,
            210,
            208,
            315,
            209,
            100,
            400,
            900,
            314,
            2200
        )
    group by
        dt,
        city_id
    union
    select
        city_id,
        dt,
        '网+出' product_name,
        sum(discount_00_to_70_cnt) as discount_00_to_70_cnt,
        sum(discount_70_to_80_cnt) as discount_70_to_80_cnt,
        sum(discount_80_to_90_cnt) as discount_80_to_90_cnt,
        sum(discount_90_to_95_cnt) as discount_90_to_95_cnt,
        sum(discount_95_to_100_cnt) as discount_95_to_100_cnt
    from
        t3
    where
        current_product_type in (
            206,
            2061,
            210,
            208,
            315,
            209,
            100,
            400,
            900,
            314,
            2200,
            500
        )
    group by
        dt,
        city_id
    union
    select
        city_id,
        dt,
        '新泛快' product_name,
        sum(discount_00_to_70_cnt) as discount_00_to_70_cnt,
        sum(discount_70_to_80_cnt) as discount_70_to_80_cnt,
        sum(discount_80_to_90_cnt) as discount_80_to_90_cnt,
        sum(discount_90_to_95_cnt) as discount_90_to_95_cnt,
        sum(discount_95_to_100_cnt) as discount_95_to_100_cnt
    from
        t3
    where
        current_product_type in (206, 2061, 314)
    group by
        dt,
        city_id
    union
    select
        city_id,
        dt,
        '泛快大盘' product_name,
        sum(discount_00_to_70_cnt) as discount_00_to_70_cnt,
        sum(discount_70_to_80_cnt) as discount_70_to_80_cnt,
        sum(discount_80_to_90_cnt) as discount_80_to_90_cnt,
        sum(discount_90_to_95_cnt) as discount_90_to_95_cnt,
        sum(discount_95_to_100_cnt) as discount_95_to_100_cnt
    from
        t3
    where
        current_product_type in (206, 2061, 210, 208, 315, 209, 314)
    group by
        dt,
        city_id
)
select
    passenger_table.city_id,
    --城市
    passenger_table.bubble_cnt,
    --冒泡数
    passenger_table.high_freq_bubble_cnt,
    --中高活跃用户冒泡数
    passenger_table.low_freq_bubble_cnt,
    --低活跃用户冒泡数
    passenger_table.xcl_freq_bubble_cnt,
    --新沉流用户冒泡数
    order_table.morning_peak_bubble_cnt,
    --早高峰冒泡数
    order_table.evening_peak_bubble_cnt,
    --晚高峰冒泡数
    order_table.night_peak_bubble_cnt,
    --夜高峰冒泡数
    discount_table.discount_00_to_70_cnt,
    --7折以下订单数
    discount_table.discount_70_to_80_cnt,
    --7折到8折订单数
    discount_table.discount_80_to_90_cnt,
    --8折到9折订单数
    discount_table.discount_90_to_95_cnt,
    --9折到95折订单数
    discount_table.discount_95_to_100_cnt,
    --95折以上订单数
    passenger_table.call_cnt,
    --发单数
    passenger_table.high_freq_call_cnt,
    --中高活跃用户发单数
    passenger_table.low_freq_call_cnt,
    --低活跃用户发单数
    passenger_table.xcl_freq_call_cnt,
    --新沉流用户发单数
    order_table.morning_peak_call_cnt,
    --早高峰发单数
    order_table.evening_peak_call_cnt,
    --晚高峰发单数
    order_table.night_peak_call_cnt,
    --夜高峰发单数
    order_table.lineup_call_cnt,
    --排队订单数
    order_table.morning_peak_lineup_call_cnt,
    --早高峰排队订单数
    order_table.evening_peak_lineup_call_cnt,
    --晚高峰排队订单数
    order_table.night_peak_lineup_call_cnt,
    --夜高峰排队订单数
    order_table.est_dur,
    --预估时长
    order_table.est_arrive_dur,
    --预计接驾时长
    order_table.morning_peak_est_arrive_dur,
    --早高峰预计接驾时长
    order_table.evening_peak_est_arrive_dur,
    --晚高峰预计接驾时长
    order_table.night_peak_est_arrive_dur,
    --夜高峰预计接驾时长
    order_table.est_arrive_dis,
    --预计接驾距离
    city_table.total_tsh,
    --TSH（没有普通快车、市内拼车，使用快车替代普通快车）
    city_table.th_yj_tsh,
    --愿接特惠时长
    city_table.answer_order_cnt,
    --应答数
    --计费时长（没有普通快车、市内拼车、特惠快车，使用快车替代普通快车和特惠快车）
    --TPH = 完单数 / TSH
    --供需比 = TSH / 发单数
    --需供比 = 发单数 / TSH
    --应答率 = 应答数 / 发单数
    passenger_table.success_cnt,
    --完单数
    passenger_table.high_freq_success_cnt,
    --中高活跃用户完单数
    passenger_table.low_freq_success_cnt,
    --低活跃用户完单数
    passenger_table.xcl_freq_success_cnt,
    --新沉流用户完单数
    order_table.morning_peak_success_cnt,
    --早高峰完单数
    order_table.evening_peak_success_cnt,
    --晚高峰完单数
    order_table.night_peak_success_cnt,
    --夜高峰完单数
    order_table.long_dis_success_cnt,
    --长距离完单数
    order_table.short_dis_success_cnt,
    --短距离完单数
    passenger_table.gmv,
    --完单GMV
    passenger_table.high_freq_gmv,
    --中高活跃用户完单GMV
    passenger_table.low_freq_gmv,
    --低活跃用户完单GMV
    passenger_table.xcl_freq_gmv,
    --新沉流用户完单GMV
    order_table.morning_peak_gmv,
    --早高峰完单GMV
    order_table.evening_peak_gmv,
    --晚高峰完单GMV
    order_table.night_peak_gmv,
    --夜高峰完单GMV
    order_table.long_dis_gmv,
    --长距离完单GMV
    order_table.short_dis_gmv,
    --短距离完单GMV
    passenger_table.driver_divide,
    --司机抽成
    passenger_table.business_profit,
    --毛利额
    passenger_table.subsidy_hufan,
    --完单呼返
    passenger_table.subsidy_c,
    --完单C补
    passenger_table.subsidy_b,
    --完单B补
    passenger_table.dt,
    --日期
    passenger_table.product_name --品类名称
from
    passenger_table full
    join city_table on passenger_table.dt = city_table.dt
    and passenger_table.city_id = city_table.city_id
    and passenger_table.product_name = city_table.product_name full
    join order_table on city_table.dt = order_table.dt
    and city_table.city_id = order_table.city_id
    and city_table.product_name = order_table.product_name full
    join discount_table on order_table.dt = discount_table.dt
    and order_table.city_id = discount_table.city_id
    and order_table.product_name = discount_table.product_name;