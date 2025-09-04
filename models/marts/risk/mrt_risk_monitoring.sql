select *
from {{ ref('int_user_risk_scores') }}
where risk_score >= 50 or bets_while_excluded > 0 or users_per_device > 2
