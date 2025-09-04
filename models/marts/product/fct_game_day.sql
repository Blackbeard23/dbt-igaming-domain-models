with b as (select * from {{ ref('fct_bet') }})
select
  cast(bet_ts as date) as game_day,
  game,
  count(*) as bets,
  sum(stake_usd) as stakes_usd,
  sum(payout_usd) as payouts_usd,
  sum(ggr_usd) as ggr_usd,
  case when sum(stake_usd) > 0 then sum(payout_usd) / sum(stake_usd) else null end as rtp
from b
group by 1,2
order by 1,2
