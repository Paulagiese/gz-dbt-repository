with sales as (
    select * from {{ ref('stg_raw_sales') }}
),

product as (
    select * from {{ ref('stg_raw_product') }}
)

select
    sales.date_date,
    sales.orders_id,
    sales.products_id,
    sales.revenue,
    sales.quantity,
    cast(product.purchase_price as float64) as purchase_price,
    sales.quantity * cast(product.purchase_price as float64) as purchase_cost,
    sales.revenue - (sales.quantity * cast(product.purchase_price as float64)) as margin
from sales
left join product
on sales.products_id = product.products_id