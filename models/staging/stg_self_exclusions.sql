select
  cast(user_id as int) as user_id,
  cast(from_ts as timestamp) as from_ts,
  cast(to_ts as timestamp) as to_ts
from {{ ref('self_exclusions') }}
