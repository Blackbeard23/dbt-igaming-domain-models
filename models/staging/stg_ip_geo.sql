select
  ip,
  upper(country) as country,
  cast(risk_score as double) as risk_score
from {{ ref('ip_geo') }}
