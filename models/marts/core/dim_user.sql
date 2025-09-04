select user_id, signup_date, country, kyc_level, device_id
from {{ ref('stg_users') }}
