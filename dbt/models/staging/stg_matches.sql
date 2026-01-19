with source as (
    select * from raw_matches
),

staged as (
    select
        match_id,
        competition,
        season,
        matchday,
        status,
        utc_date as match_date,
        home_team_id,
        home_team_name,
        away_team_id,
        away_team_name,
        home_score,
        away_score,
        winner,
        home_score + away_score as total_goals,
        abs(home_score - away_score) as goal_difference,
        case
            when home_score > away_score then 'HOME'
            when away_score > home_score then 'AWAY'
            else 'DRAW'
        end as result,
        ingested_at
    from source
    where status = 'FINISHED'
)

select * from staged