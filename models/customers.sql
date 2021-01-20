with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),
orders_payments as (

    select order_id, amount from {{ ref('stg_payments') }}

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(orders.order_id) as number_of_orders,
        sum(orders_payments.amount) as Customer_LTV

    from orders left outer join orders_payments on (orders.order_id=orders_payments.order_id)

    group by 1

),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        Customer_LTV

    from customers

    left join customer_orders using (customer_id)

)

select * from final