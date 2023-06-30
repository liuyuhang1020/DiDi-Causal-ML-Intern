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
    ) as night_peak_bubble_cnt, --夜高峰冒泡数
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
    level_3_product;