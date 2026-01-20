with matches as (
    select * from {{ ref('stg_matches') }}
),

monthly as (
    select
        date_trunc('month', match_date::date) as month,
        count(*) as matches_played,
        sum(total_goals) as total_goals,
        sum(home_score) as home_goals,
        sum(away_score) as away_goals,
        round(avg(total_goals), 2) as avg_goals_per_match,
        sum(case when total_goals = 0 then 1 else 0 end) as scoreless_draws,
        sum(case when total_goals >= 4 then 1 else 0 end) as high_scoring_games,
        max(total_goals) as max_goals_in_match
    from matches
    group by 1
)

select
    *,
    round(home_goals::decimal / nullif(total_goals, 0) * 100, 1) as home_goal_pct,
    round(high_scoring_games::decimal / nullif(matches_played, 0) * 100, 1) as high_scoring_pct
from monthly
order by month
