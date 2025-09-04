select
  cast(user_id as int) as user_id,
  cast(signup_date as date) as signup_date,
  upper(country) as country,
  lower(kyc_level) as kyc_level,
  device_id
from {{ ref('users') }}
