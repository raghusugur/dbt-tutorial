select
    orderid as order_id,
    Id as payment_id,
    amount/100 as amount
    
from {{ source('stripe', 'payment') }}
where status='success'