from airflow.providers.postgres.hooks.postgres import PostgresHook

def get_pg_connection():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    return hook.get_conn()

def execute_sql(sql, params=None):
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    cur.close()
    conn.close()

def create_matches_table():
    sql = """
    CREATE TABLE IF NOT EXISTS raw_matches (
        match_id INTEGER PRIMARY KEY,
        competition VARCHAR(10),
        season INTEGER,
        matchday INTEGER,
        status VARCHAR(20),
        utc_date TIMESTAMP,
        home_team_id INTEGER,
        home_team_name VARCHAR(100),
        away_team_id INTEGER,
        away_team_name VARCHAR(100),
        home_score INTEGER,
        away_score INTEGER,
        winner VARCHAR(20),
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    execute_sql(sql)

def upsert_match(match):
    sql = """
    INSERT INTO raw_matches (
        match_id, competition, season, matchday, status, utc_date,
        home_team_id, home_team_name, away_team_id, away_team_name,
        home_score, away_score, winner
    ) VALUES (
        %(match_id)s, %(competition)s, %(season)s, %(matchday)s, 
        %(status)s, %(utc_date)s, %(home_team_id)s, %(home_team_name)s,
        %(away_team_id)s, %(away_team_name)s, %(home_score)s, 
        %(away_score)s, %(winner)s
    )
    ON CONFLICT (match_id) DO UPDATE SET
        status = EXCLUDED.status,
        home_score = EXCLUDED.home_score,
        away_score = EXCLUDED.away_score,
        winner = EXCLUDED.winner,
        ingested_at = CURRENT_TIMESTAMP;
    """
    execute_sql(sql, match)

def create_standings_table():
    sql = """
    CREATE TABLE IF NOT EXISTS raw_standings (
        id SERIAL PRIMARY KEY,
        competition VARCHAR(10),
        season INTEGER,
        matchday INTEGER,
        team_id INTEGER,
        team_name VARCHAR(100),
        position INTEGER,
        played INTEGER,
        won INTEGER,
        draw INTEGER,
        lost INTEGER,
        goals_for INTEGER,
        goals_against INTEGER,
        goal_difference INTEGER,
        points INTEGER,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(competition, season, team_id)
    );
    """
    execute_sql(sql)

def upsert_standing(standing):
    sql = """
    INSERT INTO raw_standings (
        competition, season, matchday, team_id, team_name, position,
        played, won, draw, lost, goals_for, goals_against,
        goal_difference, points
    ) VALUES (
        %(competition)s, %(season)s, %(matchday)s, %(team_id)s,
        %(team_name)s, %(position)s, %(played)s, %(won)s, %(draw)s,
        %(lost)s, %(goals_for)s, %(goals_against)s,
        %(goal_difference)s, %(points)s
    )
    ON CONFLICT (competition, season, team_id) DO UPDATE SET
        matchday = EXCLUDED.matchday,
        position = EXCLUDED.position,
        played = EXCLUDED.played,
        won = EXCLUDED.won,
        draw = EXCLUDED.draw,
        lost = EXCLUDED.lost,
        goals_for = EXCLUDED.goals_for,
        goals_against = EXCLUDED.goals_against,
        goal_difference = EXCLUDED.goal_difference,
        points = EXCLUDED.points,
        ingested_at = CURRENT_TIMESTAMP;
    """
    execute_sql(sql, standing)

def create_scorers_table():
    sql = """
    CREATE TABLE IF NOT EXISTS raw_scorers (
        id SERIAL PRIMARY KEY,
        competition VARCHAR(10),
        season INTEGER,
        player_id INTEGER,
        player_name VARCHAR(100),
        nationality VARCHAR(50),
        team_id INTEGER,
        team_name VARCHAR(100),
        goals INTEGER,
        assists INTEGER,
        penalties INTEGER,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(competition, season, player_id)
    );
    """
    execute_sql(sql)

def upsert_scorer(scorer):
    sql = """
    INSERT INTO raw_scorers (
        competition, season, player_id, player_name, nationality,
        team_id, team_name, goals, assists, penalties
    ) VALUES (
        %(competition)s, %(season)s, %(player_id)s, %(player_name)s,
        %(nationality)s, %(team_id)s, %(team_name)s, %(goals)s,
        %(assists)s, %(penalties)s
    )
    ON CONFLICT (competition, season, player_id) DO UPDATE SET
        goals = EXCLUDED.goals,
        assists = EXCLUDED.assists,
        penalties = EXCLUDED.penalties,
        team_id = EXCLUDED.team_id,
        team_name = EXCLUDED.team_name,
        ingested_at = CURRENT_TIMESTAMP;
    """
    execute_sql(sql, scorer)

def create_teams_table():
    sql = """
    CREATE TABLE IF NOT EXISTS raw_teams (
        team_id INTEGER PRIMARY KEY,
        team_name VARCHAR(100),
        short_name VARCHAR(50),
        tla VARCHAR(5),
        crest_url VARCHAR(255),
        address VARCHAR(255),
        website VARCHAR(255),
        founded INTEGER,
        venue VARCHAR(100),
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    execute_sql(sql)

def upsert_team(team):
    sql = """
    INSERT INTO raw_teams (
        team_id, team_name, short_name, tla, crest_url,
        address, website, founded, venue
    ) VALUES (
        %(team_id)s, %(team_name)s, %(short_name)s, %(tla)s,
        %(crest_url)s, %(address)s, %(website)s, %(founded)s, %(venue)s
    )
    ON CONFLICT (team_id) DO UPDATE SET
        team_name = EXCLUDED.team_name,
        short_name = EXCLUDED.short_name,
        crest_url = EXCLUDED.crest_url,
        venue = EXCLUDED.venue,
        ingested_at = CURRENT_TIMESTAMP;
    """
    execute_sql(sql, team)