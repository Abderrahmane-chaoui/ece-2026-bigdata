-- A user cannot order before being born: the test fails if this query returns rows.
-- Since exercise 3, stg_users replaces an invalid birthdate with NULL, so the test passes
-- and is an error again (no more severity warn).
select
    o.order_id,
    o.ordered_at,
    u.user_id,
    u.birthdate
from {{ ref('stg_orders') }} o
join {{ ref('stg_users') }} u on o.user_id = u.user_id
where o.ordered_at < u.birthdate
