with b as (select * from {{ ref('int_exclusion_flags') }}),
geo as (select * from {{ ref('stg_ip_geo') }})
select
  b.bet_id, b.user_id, b.game, b.bet_ts, b.stake_usd, b.outcome, b.payout_usd, b.bonus_used, b.ip, b.device_id, b.is_self_excluded,
  g.country as ip_country,
  g.risk_score as ip_risk_score,
  case when b.is_self_excluded then 0 else b.stake_usd end as valid_stake_usd,
  case when b.is_self_excluded then 0 else b.payout_usd end as valid_payout_usd,
  case when b.is_self_excluded then 0 else (b.stake_usd - b.payout_usd) end as ggr_usd,
  case when b.bonus_used=1 then b.stake_usd else 0 end as bonus_stake_usd,
  case when b.bonus_used=0 then b.stake_usd else 0 end as cash_stake_usd
from b
left join geo g on g.ip = b.ip
