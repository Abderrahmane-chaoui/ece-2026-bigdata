-- Exercise 2: an order must contain at least one item
select order_id, quantity
from {{ ref('stg_orders') }}
where quantity is null or quantity <= 0
