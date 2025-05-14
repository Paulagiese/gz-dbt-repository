with sales_margin as (
    select * from {{ ref('int_sales_margin') }}
)

select
    orders_id,
    min(date_date) as date_date,
    sum(margin) as margin
from sales_margin
group by orders_id