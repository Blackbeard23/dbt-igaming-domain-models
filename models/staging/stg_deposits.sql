select
  cast(deposit_id as int) as deposit_id,
  cast(user_id as int) as user_id,
  cast(ts as timestamp) as ts,
  cast(amount_usd as double) as amount_usd,
  lower(method) as method,
  lower(status) as status
from {{ ref('deposits') }}
