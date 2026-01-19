with source as (
    select * from raw_teams
),

staged as (
    select
        team_id,
        team_name,
        short_name,
        tla,
        crest_url,
        address,
        website,
        founded,
        venue,
        2024 - founded as years_since_founding,
        ingested_at
    from source
)

select * from staged