with matches_unpivoted as (
    -- Home matches
    select
        match_date,
        matchday,
        home_team_id as team_id,
        home_team_name as team_name,
        case result
            when 'HOME' then 'W'
            when 'AWAY' then 'L'
            else 'D'
        end as result_code,
        case result
            when 'HOME' then 3
            when 'DRAW' then 1
            else 0
        end as points,
        home_score as goals_for,
        away_score as goals_against
    from {{ ref('stg_matches') }}
    
    union all
    
    -- Away matches
    select
        match_date,
        matchday,
        away_team_id as team_id,
        away_team_name as team_name,
        case result
            when 'AWAY' then 'W'
            when 'HOME' then 'L'
            else 'D'
        end as result_code,
        case result
            when 'AWAY' then 3
            when 'DRAW' then 1
            else 0
        end as points,
        away_score as goals_for,
        home_score as goals_against
    from {{ ref('stg_matches') }}
),

ranked as (
    select
        *,
        row_number() over (partition by team_id order by match_date desc) as match_recency
    from matches_unpivoted
),

last_5 as (
    select * from ranked where match_recency <= 5
),

form_aggregated as (
    select
        team_id,
        team_name,
        sum(points) as form_points,
        sum(case when result_code = 'W' then 1 else 0 end) as form_wins,
        sum(case when result_code = 'D' then 1 else 0 end) as form_draws,
        sum(case when result_code = 'L' then 1 else 0 end) as form_losses,
        sum(goals_for) as form_goals_for,
        sum(goals_against) as form_goals_against,
        -- Form string like "WWDLW"
        string_agg(result_code, '' order by match_recency) as form_string
    from last_5
    group by team_id, team_name
)

select
    *,
    form_goals_for - form_goals_against as form_goal_difference,
    rank() over (order by form_points desc, form_goals_for - form_goals_against desc) as form_rank
from form_aggregated
order by form_points desc
