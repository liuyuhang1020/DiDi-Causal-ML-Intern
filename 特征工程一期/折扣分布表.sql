with t as(
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
)
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
    t
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
    t
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
    t
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
    t
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
    t
where
    current_product_type in (206, 2061, 210, 208, 315, 209, 314)
group by
    dt,
    city_id;