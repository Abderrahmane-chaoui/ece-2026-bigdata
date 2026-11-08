-- Exercise 3: an invalid birthdate must be NULL, a valid one must be kept
select user_id, birthdate, birthdate_is_valid
from {{ ref('stg_users') }}
where (not birthdate_is_valid and birthdate is not null)
