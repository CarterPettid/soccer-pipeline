import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# Database connection
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port="5432"
    )

def run_query(query):
    conn = get_connection()
    return pd.read_sql(query, conn)

# Page config
st.set_page_config(
    page_title="Premier League Analytics",
    page_icon="⚽",
    layout="wide"
)

st.title("⚽ Premier League Analytics Dashboard")
st.markdown("---")

# Sidebar
st.sidebar.header("Navigation")
page = st.sidebar.radio(
    "Select Page", 
    ["League Table", "Form Table", "Top Scorers", "Team Performance", 
     "Head to Head", "Monthly Trends", "Teams Directory", "Recent Matches"]
)

# League Table Page
if page == "League Table":
    st.header("📊 League Standings")
    
    query = """
    SELECT position, team_name, played, won, draw, lost, 
           goals_for, goals_against, goal_difference, points
    FROM stg_standings
    ORDER BY position
    """
    df = run_query(query)
    
    st.dataframe(df, use_container_width=True, hide_index=True)
    
    # Points bar chart
    st.subheader("Points by Team")
    fig = px.bar(df, x="team_name", y="points", color="points",
                 color_continuous_scale="greens")
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

# Form Table Page
elif page == "Form Table":
    st.header("🔥 Form Table (Last 5 Matches)")
    
    query = """
    SELECT form_rank, team_name, form_string, form_points, 
           form_wins, form_draws, form_losses,
           form_goals_for, form_goals_against, form_goal_difference
    FROM fct_form_table
    ORDER BY form_rank
    """
    df = run_query(query)
    
    # Style the form string with colors
    st.dataframe(df, use_container_width=True, hide_index=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Form Points Distribution")
        fig = px.bar(df.head(10), x="team_name", y="form_points", 
                     color="form_points", color_continuous_scale="RdYlGn",
                     title="Top 10 Teams by Form")
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Form: Goals Scored vs Conceded")
        fig = px.scatter(df, x="form_goals_for", y="form_goals_against", 
                         text="team_name", color="form_points",
                         color_continuous_scale="RdYlGn",
                         title="Goals For vs Against (Last 5 Games)")
        fig.update_traces(textposition="top center")
        st.plotly_chart(fig, use_container_width=True)

# Top Scorers Page
elif page == "Top Scorers":
    st.header("🥇 Top Scorers")
    
    query = """
    SELECT goals_rank as rank, player_name, team_name, goals, assists, 
           penalties, non_penalty_goals, goal_contributions,
           penalty_pct, team_goals_rank
    FROM fct_player_performance
    ORDER BY goals DESC
    LIMIT 20
    """
    df = run_query(query)
    
    st.dataframe(df, use_container_width=True, hide_index=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Goals vs Assists")
        fig = px.scatter(df, x="goals", y="assists", text="player_name",
                         size="goal_contributions", color="team_name",
                         title="Goal Contributions Breakdown")
        fig.update_traces(textposition="top center")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Non-Penalty vs Penalty Goals")
        fig = px.bar(df.head(10), x="player_name", y=["non_penalty_goals", "penalties"],
                     title="Top 10: Goals Breakdown",
                     labels={"value": "Goals", "variable": "Type"})
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

# Team Performance Page
elif page == "Team Performance":
    st.header("📈 Team Performance")
    
    query = """
    SELECT team_name, total_games, total_wins, total_draws, total_losses,
           total_goals_for, total_goals_against, goal_difference,
           total_points, points_per_game,
           home_wins, away_wins
    FROM fct_team_performance
    ORDER BY total_points DESC
    """
    df = run_query(query)
    
    st.dataframe(df, use_container_width=True, hide_index=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Home Wins vs Away Wins")
        fig = px.scatter(df, x="home_wins", y="away_wins", text="team_name",
                         size="total_points", color="total_points",
                         color_continuous_scale="greens")
        fig.update_traces(textposition="top center")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Points Per Game")
        fig = px.bar(df, x="team_name", y="points_per_game", 
                     color="points_per_game", color_continuous_scale="greens")
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

# Head to Head Page
elif page == "Head to Head":
    st.header("⚔️ Head to Head")
    
    # Get all teams for selection
    teams_query = "SELECT DISTINCT team_name FROM dim_teams ORDER BY team_name"
    teams_df = run_query(teams_query)
    teams_list = teams_df["team_name"].tolist()
    
    col1, col2 = st.columns(2)
    with col1:
        team_a = st.selectbox("Select Team A", teams_list, index=0)
    with col2:
        team_b = st.selectbox("Select Team B", teams_list, index=1 if len(teams_list) > 1 else 0)
    
    if team_a and team_b and team_a != team_b:
        query = f"""
        SELECT team_a_name, team_b_name, total_matches,
               team_a_wins, draws, team_b_wins,
               team_a_goals, team_b_goals, total_goals,
               avg_goals_per_match, first_meeting, last_meeting
        FROM fct_head_to_head
        WHERE (team_a_name = '{team_a}' AND team_b_name = '{team_b}')
           OR (team_a_name = '{team_b}' AND team_b_name = '{team_a}')
        """
        df = run_query(query)
        
        if not df.empty:
            row = df.iloc[0]
            
            # Determine which is team A/B in results
            if row['team_a_name'] == team_a:
                t1_wins, t2_wins = row['team_a_wins'], row['team_b_wins']
                t1_goals, t2_goals = row['team_a_goals'], row['team_b_goals']
            else:
                t1_wins, t2_wins = row['team_b_wins'], row['team_a_wins']
                t1_goals, t2_goals = row['team_b_goals'], row['team_a_goals']
            
            st.subheader(f"{team_a} vs {team_b}")
            
            col1, col2, col3 = st.columns(3)
            col1.metric(f"{team_a} Wins", int(t1_wins))
            col2.metric("Draws", int(row['draws']))
            col3.metric(f"{team_b} Wins", int(t2_wins))
            
            col1, col2, col3 = st.columns(3)
            col1.metric(f"{team_a} Goals", int(t1_goals))
            col2.metric("Total Matches", int(row['total_matches']))
            col3.metric(f"{team_b} Goals", int(t2_goals))
            
            st.markdown(f"**Avg Goals/Match:** {row['avg_goals_per_match']}")
            st.markdown(f"**First Meeting:** {row['first_meeting']} | **Last Meeting:** {row['last_meeting']}")
            
            # Win distribution pie chart
            fig = px.pie(
                values=[t1_wins, row['draws'], t2_wins],
                names=[f"{team_a} Wins", "Draws", f"{team_b} Wins"],
                title="Results Distribution",
                color_discrete_sequence=["#2ecc71", "#95a5a6", "#e74c3c"]
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No head-to-head data found for these teams.")
    elif team_a == team_b:
        st.warning("Please select two different teams.")

# Monthly Trends Page
elif page == "Monthly Trends":
    st.header("📅 Monthly Goal Trends")
    
    query = """
    SELECT month, matches_played, total_goals, home_goals, away_goals,
           avg_goals_per_match, scoreless_draws, high_scoring_games,
           max_goals_in_match, home_goal_pct, high_scoring_pct
    FROM fct_monthly_goals
    ORDER BY month
    """
    df = run_query(query)
    
    st.dataframe(df, use_container_width=True, hide_index=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Goals Over Time")
        fig = px.line(df, x="month", y="total_goals", markers=True,
                      title="Total Goals by Month")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Average Goals Per Match")
        fig = px.bar(df, x="month", y="avg_goals_per_match",
                     color="avg_goals_per_match", color_continuous_scale="Oranges",
                     title="Avg Goals/Match by Month")
        st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Home vs Away Goals")
        fig = px.bar(df, x="month", y=["home_goals", "away_goals"],
                     title="Home vs Away Goals by Month",
                     labels={"value": "Goals", "variable": "Location"})
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("High Scoring Games %")
        fig = px.area(df, x="month", y="high_scoring_pct",
                      title="% of Matches with 4+ Goals")
        st.plotly_chart(fig, use_container_width=True)

# Teams Directory Page
elif page == "Teams Directory":
    st.header("🏟️ Teams Directory")
    
    query = """
    SELECT team_name, short_name, venue, founded, years_since_founding,
           current_position, current_points, matches_played, league_tier
    FROM dim_teams
    ORDER BY current_position
    """
    df = run_query(query)
    
    # Filter by league tier
    tier_filter = st.multiselect(
        "Filter by League Tier",
        options=df["league_tier"].unique().tolist(),
        default=df["league_tier"].unique().tolist()
    )
    
    filtered_df = df[df["league_tier"].isin(tier_filter)]
    st.dataframe(filtered_df, use_container_width=True, hide_index=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Teams by Founding Year")
        fig = px.bar(filtered_df.sort_values("founded"), x="team_name", y="founded",
                     color="league_tier", title="Club Founding Years")
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("League Tier Distribution")
        tier_counts = filtered_df["league_tier"].value_counts()
        fig = px.pie(values=tier_counts.values, names=tier_counts.index,
                     title="Teams by League Position Tier",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        st.plotly_chart(fig, use_container_width=True)

# Recent Matches Page
elif page == "Recent Matches":
    st.header("📅 Recent Matches")
    
    query = """
    SELECT match_date, home_team_name, home_score, away_score, 
           away_team_name, result, total_goals
    FROM stg_matches
    ORDER BY match_date DESC
    LIMIT 50
    """
    df = run_query(query)
    
    st.dataframe(df, use_container_width=True, hide_index=True)
    
    # Goals distribution
    st.subheader("Total Goals per Match Distribution")
    fig = px.histogram(df, x="total_goals", nbins=10)
    st.plotly_chart(fig, use_container_width=True)

# Footer
st.markdown("---")
st.caption("Data sourced from Football-Data.org API | Pipeline built with Airflow + dbt")
