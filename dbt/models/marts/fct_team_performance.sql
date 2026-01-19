with home_stats as (
    select
        home_team_id as team_id,
        home_team_name as team_name,
        count(*) as home_games,
        sum(case when result = 'HOME' then 1 else 0 end) as home_wins,
        sum(case when result = 'DRAW' then 1 else 0 end) as home_draws,
        sum(case when result = 'AWAY' then 1 else 0 end) as home_losses,
        sum(home_score) as home_goals_for,
        sum(away_score) as home_goals_against
    from {{ ref('stg_matches') }}
    group by home_team_id, home_team_name
),

away_stats as (
    select
        away_team_id as team_id,
        away_team_name as team_name,
        count(*) as away_games,
        sum(case when result = 'AWAY' then 1 else 0 end) as away_wins,
        sum(case when result = 'DRAW' then 1 else 0 end) as away_draws,
        sum(case when result = 'HOME' then 1 else 0 end) as away_losses,
        sum(away_score) as away_goals_for,
        sum(home_score) as away_goals_against
    from {{ ref('stg_matches') }}
    group by away_team_id, away_team_name
),

combined as (
    select
        h.team_id,
        h.team_name,
        h.home_games + a.away_games as total_games,
        h.home_wins + a.away_wins as total_wins,
        h.home_draws + a.away_draws as total_draws,
        h.home_losses + a.away_losses as total_losses,
        h.home_goals_for + a.away_goals_for as total_goals_for,
        h.home_goals_against + a.away_goals_against as total_goals_against,
        (h.home_wins + a.away_wins) * 3 + (h.home_draws + a.away_draws) as total_points,
        h.home_wins,
        h.home_draws,
        h.home_losses,
        a.away_wins,
        a.away_draws,
        a.away_losses
    from home_stats h
    join away_stats a on h.team_id = a.team_id
)

select
    *,
    total_goals_for - total_goals_against as goal_difference,
    round(total_points::decimal / nullif(total_games, 0), 2) as points_per_game
from combined
order by total_points desc, goal_difference desc