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
    ) as short_dis_gmv, --短距离完单GMV
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
    level_3_product;