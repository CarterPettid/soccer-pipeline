with matches as (
    select * from {{ ref('stg_matches') }}
),

head_to_head as (
    select
        -- Always put alphabetically first team as team_a for consistency
        case when home_team_name < away_team_name then home_team_id else away_team_id end as team_a_id,
        case when home_team_name < away_team_name then home_team_name else away_team_name end as team_a_name,
        case when home_team_name < away_team_name then away_team_id else home_team_id end as team_b_id,
        case when home_team_name < away_team_name then away_team_name else home_team_name end as team_b_name,
        count(*) as total_matches,
        sum(case 
            when (home_team_name < away_team_name and result = 'HOME') 
              or (home_team_name > away_team_name and result = 'AWAY') 
            then 1 else 0 
        end) as team_a_wins,
        sum(case when result = 'DRAW' then 1 else 0 end) as draws,
        sum(case 
            when (home_team_name < away_team_name and result = 'AWAY') 
              or (home_team_name > away_team_name and result = 'HOME') 
            then 1 else 0 
        end) as team_b_wins,
        sum(case when home_team_name < away_team_name then home_score else away_score end) as team_a_goals,
        sum(case when home_team_name < away_team_name then away_score else home_score end) as team_b_goals,
        max(match_date) as last_meeting,
        min(match_date) as first_meeting
    from matches
    group by 1, 2, 3, 4
)

select
    *,
    team_a_goals + team_b_goals as total_goals,
    round((team_a_goals + team_b_goals)::decimal / nullif(total_matches, 0), 2) as avg_goals_per_match
from head_to_head
order by total_matches desc
