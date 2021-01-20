select
    orderid as order_id,
    Id as payment_id,
    amount/100 as amount
    
from raw.Stripe.payment
where status='success'