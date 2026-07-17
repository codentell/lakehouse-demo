select
    payment_id,
    order_id,
    method,
    paid_amount
from {{ source('lakehouse_demo', 'silver_payments') }}
