with scorers as (
    select * from {{ ref('stg_scorers') }}
),

teams as (
    select team_id, short_name, venue from {{ ref('stg_teams') }}
),

enriched as (
    select
        s.player_id,
        s.player_name,
        s.nationality,
        s.team_id,
        s.team_name,
        t.short_name as team_short_name,
        t.venue,
        s.goals,
        s.assists,
        s.penalties,
        s.non_penalty_goals,
        s.goal_contributions,
        -- Efficiency metrics
        round(s.penalties::decimal / nullif(s.goals, 0) * 100, 1) as penalty_pct,
        -- Rankings
        rank() over (order by s.goals desc) as goals_rank,
        rank() over (order by s.assists desc) as assists_rank,
        rank() over (order by s.goal_contributions desc) as contributions_rank,
        rank() over (partition by s.team_id order by s.goals desc) as team_goals_rank
    from scorers s
    left join teams t on s.team_id = t.team_id
)

select * from enriched
