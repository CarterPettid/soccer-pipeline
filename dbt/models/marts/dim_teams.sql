with teams as (
    select * from {{ ref('stg_teams') }}
),

standings as (
    select * from {{ ref('stg_standings') }}
),

enriched as (
    select
        t.team_id,
        t.team_name,
        t.short_name,
        t.tla,
        t.crest_url,
        t.address,
        t.website,
        t.founded,
        t.venue,
        t.years_since_founding,
        -- Current standings info
        s.position as current_position,
        s.points as current_points,
        s.played as matches_played,
        -- Tier classification
        case
            when s.position <= 4 then 'Champions League'
            when s.position <= 6 then 'Europa League'
            when s.position >= 18 then 'Relegation Zone'
            else 'Mid-Table'
        end as league_tier
    from teams t
    left join standings s on t.team_id = s.team_id
)

select * from enriched
