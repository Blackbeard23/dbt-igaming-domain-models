select
  cast(withdraw_id as int) as withdraw_id,
  cast(user_id as int) as user_id,
  cast(ts as timestamp) as ts,
  cast(amount_usd as double) as amount_usd,
  lower(status) as status
from {{ ref('withdrawals') }}
