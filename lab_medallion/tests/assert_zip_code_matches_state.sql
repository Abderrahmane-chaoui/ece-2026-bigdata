{{ config(severity='warn') }}

-- Exercise 4: the ZIP code must belong to one of the ranges of the state.
-- Reference data: seeds/state_zip_ranges.csv (USPS ZIP prefixes per state).
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
where not exists (
    select 1
    from {{ ref('state_zip_ranges') }} r
    where r.state = u.state
      and cast(u.zip_code as integer) between r.zip_min and r.zip_max
)
