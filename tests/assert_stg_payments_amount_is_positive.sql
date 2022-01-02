with 

payments as (
SELECT * FROM {{ref('stg_payments')}}
)

SELECT 
order_id,
SUM(amount) as total_amount
from payments
group by order_id
having total_amount < 0