select
  cast(bet_id as int) as bet_id,
  cast(user_id as int) as user_id,
  lower(game) as game,
  cast(bet_ts as timestamp) as bet_ts,
  cast(stake_usd as double) as stake_usd,
  lower(outcome) as outcome,
  cast(payout_usd as double) as payout_usd,
  cast(bonus_used as int) as bonus_used,
  ip,
  device_id
from {{ ref('bets') }}
