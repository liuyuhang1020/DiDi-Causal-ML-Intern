with t as (
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
)
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
                    t
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
                    t
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
            t
    ) bb on aa.dt = bb.dt
    and aa.city_id = bb.city_id
    and aa.src_product_name = bb.product_name;