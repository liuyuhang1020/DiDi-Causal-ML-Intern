with temp as(
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
)
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
    temp
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
    temp
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
    temp
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
    temp
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
    temp
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
    temp
where
    level_2_product in (110100, 110200, 110400, 110800, 110900)
group by
    city_id,
    dt;