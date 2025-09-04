with exc as (select * from {{ ref('stg_self_exclusions') }}),
bets as (select * from {{ ref('stg_bets') }})
select
  b.*,
  case when exists (
    select 1 from exc e
    where e.user_id = b.user_id
      and b.bet_ts between e.from_ts and e.to_ts
  )
  then true else false end as is_self_excluded
from bets b
