with b as (select * from {{ ref('fct_bet') }}),
agg as (
  select user_id, count(*) as bets, sum(ggr_usd) as ggr_usd
  from b
  group by 1
)
select
  a.user_id,
  a.bets,
  a.ggr_usd,
  case
    when a.ggr_usd >= 200 then 'VIP'
    when a.ggr_usd between 50 and 199 then 'HIGH'
    when a.ggr_usd between 10 and 49 then 'MEDIUM'
    else 'LOW'
  end as vip_tier
from agg a
