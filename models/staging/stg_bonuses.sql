select
  cast(bonus_id as int) as bonus_id,
  cast(user_id as int) as user_id,
  cast(issued_ts as timestamp) as issued_ts,
  lower(type) as type,
  cast(value_usd as double) as value_usd,
  cast(wagering_req_x as int) as wagering_req_x
from {{ ref('bonuses') }}
