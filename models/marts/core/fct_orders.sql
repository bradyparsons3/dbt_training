 
with orders as (
    select * from {{ref('stg_orders')}}
),

payments as (
    select * from {{ref('stg_payments')}}
), 
 
 customer_orders as ( 
 
    select 
        order_id,
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        Amount 
   from orders
   left join payments using (order_id)
   group by 1,2,6

   ),
   
   final as ( 
   
    select 
       customer_orders.order_id,
       customer_orders.customer_id,
       first_order_date,
       most_recent_order_date,
       number_of_orders,
       SUM(customer_orders.Amount) as revenue
     from customer_orders
     group by 1,2,3,4,5
     
     
  )
  
  select * from final 