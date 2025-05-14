with operational as (
    select * from {{ ref('int_orders_operational') }}
),

sales_margin as (
    select * from {{ ref('int_sales_margin') }}
)

select
    operational.date_date,
    count(distinct operational.orders_id) as nb_transactions,
    sum(sales_margin.revenue) as revenue,
    round(sum(sales_margin.revenue) / count(distinct operational.orders_id), 2) as average_basket,
    round(sum(sales_margin.margin), 2) as margin,
    round(sum(operational.operational_margin), 2) as operational_margin
from operational
left join sales_margin
on operational.orders_id = sales_margin.orders_id
group by operational.date_date
order by operational.date_date