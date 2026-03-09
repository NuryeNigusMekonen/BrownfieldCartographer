with raw_orders as (
    select * from orders_raw
),
renamed as (
    select order_id, customer_id from raw_orders
)
select * from renamed
