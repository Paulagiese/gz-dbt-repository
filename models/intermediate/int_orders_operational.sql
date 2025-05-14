with margin as (
    select * from {{ ref('int_orders_margin') }}
),

shipping as (
    select
        orders_id,
        cast(ship_cost as float64) as ship_cost,
        shipping_fee,
        logCost
    from {{ ref('stg_raw_ship') }}
)

select
    margin.orders_id,
    margin.date_date,
    margin.margin + shipping.shipping_fee - shipping.logCost - shipping.ship_cost as operational_margin
from margin
left join shipping
on margin.orders_id = shipping.orders_id