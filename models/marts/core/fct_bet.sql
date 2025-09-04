select
  bet_id, user_id, game, bet_ts,
  valid_stake_usd as stake_usd,
  valid_payout_usd as payout_usd,
  ggr_usd,
  bonus_stake_usd,
  cash_stake_usd,
  ip_country,
  ip_risk_score
from {{ ref('int_bets_enriched') }}
