with bets as (select * from {{ ref('int_bets_enriched') }}),
deps as (select * from {{ ref('stg_deposits') }} where status='completed'),
wds as (select * from {{ ref('stg_withdrawals') }} where status='completed'),
users as (select * from {{ ref('stg_users') }}),
agg_devices as (
  select device_id, count(distinct user_id) as users_per_device
  from users group by 1
),
cashflows as (
  select
    u.user_id,
    coalesce(sum(d.amount_usd),0) as deposits_usd,
    coalesce(sum(w.amount_usd),0) as withdrawals_usd
  from users u
  left join deps d on d.user_id=u.user_id
  left join wds w on w.user_id=u.user_id
  group by 1
),
bet_flags as (
  select
    user_id,
    avg(ip_risk_score) as avg_ip_risk,
    sum(case when is_self_excluded then 1 else 0 end) as bets_while_excluded,
    count(*) as bet_count
  from bets
  group by 1
)
select
  u.user_id,
  u.device_id,
  coalesce(cf.deposits_usd,0) as deposits_usd,
  coalesce(cf.withdrawals_usd,0) as withdrawals_usd,
  coalesce(bf.avg_ip_risk,0) as avg_ip_risk,
  coalesce(bf.bets_while_excluded,0) as bets_while_excluded,
  coalesce(bf.bet_count,0) as bet_count,
  ad.users_per_device,
  round(least(100,
      20*coalesce(bf.avg_ip_risk,0)
    + 10*case when coalesce(bf.bets_while_excluded,0)>0 then 1 else 0 end
    + 15*case when coalesce(ad.users_per_device,1)>2 then 1 else 0 end
    + 10*case when coalesce(cf.withdrawals_usd,0) > coalesce(cf.deposits_usd,0)*0.9 then 1 else 0 end
  ), 2) as risk_score
from users u
left join agg_devices ad using (device_id)
left join cashflows cf using (user_id)
left join bet_flags bf using (user_id)
