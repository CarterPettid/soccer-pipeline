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
page = st.sidebar.radio("Select Page", ["League Table", "Top Scorers", "Team Performance", "Recent Matches"])

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

# Top Scorers Page
elif page == "Top Scorers":
    st.header("🥇 Top Scorers")
    
    query = """
    SELECT player_name, team_name, goals, assists, 
           penalties, non_penalty_goals, goal_contributions
    FROM stg_scorers
    ORDER BY goals DESC
    LIMIT 20
    """
    df = run_query(query)
    
    st.dataframe(df, use_container_width=True, hide_index=True)
    
    # Goals vs Assists scatter
    st.subheader("Goals vs Assists")
    fig = px.scatter(df, x="goals", y="assists", text="player_name",
                     size="goal_contributions", color="team_name")
    fig.update_traces(textposition="top center")
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
    
    # Home vs Away wins
    st.subheader("Home Wins vs Away Wins")
    fig = px.scatter(df, x="home_wins", y="away_wins", text="team_name",
                     size="total_points", color="total_points",
                     color_continuous_scale="greens")
    fig.update_traces(textposition="top center")
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