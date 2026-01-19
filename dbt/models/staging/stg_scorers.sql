with source as (
    select * from raw_scorers
),

staged as (
    select
        player_id,
        player_name,
        nationality,
        team_id,
        team_name,
        goals,
        assists,
        penalties,
        goals - penalties as non_penalty_goals,
        goals + assists as goal_contributions,
        ingested_at
    from source
)

select * from staged