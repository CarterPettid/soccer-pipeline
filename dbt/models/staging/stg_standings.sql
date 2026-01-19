with source as (
    select * from raw_standings
),

staged as (
    select
        id,
        competition,
        season,
        matchday,
        team_id,
        team_name,
        position,
        played,
        won,
        draw,
        lost,
        goals_for,
        goals_against,
        goal_difference,
        points,
        round(points::decimal / nullif(played, 0), 2) as points_per_game,
        round(goals_for::decimal / nullif(played, 0), 2) as goals_per_game,
        ingested_at
    from source
)

select * from staged