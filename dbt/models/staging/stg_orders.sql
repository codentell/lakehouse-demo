-- Staging: rename/standardize only. Business logic lives in marts.
select
    order_id,
    collector_id,
    order_ts,
    cast(order_ts as date) as order_date,
    amount,
    status
from {{ source('lakehouse_demo', 'silver_orders') }}
