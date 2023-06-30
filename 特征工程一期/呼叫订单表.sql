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
    level_3_product;