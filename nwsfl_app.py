import streamlit as st
import streamlit_authenticator as stauth
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import yaml
from yaml.loader import SafeLoader

load_dotenv()

# Page config
st.set_page_config(page_title="NWSFL", page_icon="⚽", layout="wide")

# Load authentication config
if os.path.exists('config.yaml'):
    with open('config.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)
else:
    # For Streamlit Cloud - use secrets (convert to mutable dict)
    config = {
        'credentials': {
            'usernames': {
                username: dict(user_data)
                for username, user_data in st.secrets['credentials']['usernames'].items()
            }
        },
        'cookie': {
            'name': st.secrets['cookie']['name'],
            'key': st.secrets['cookie']['key'],
            'expiry_days': st.secrets['cookie']['expiry_days']
        }
    }

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

# Database connection
def get_database_url():
    """Get the database URL from secrets or environment."""
    return st.secrets.get("DATABASE_URL") if "DATABASE_URL" in st.secrets else os.getenv('DATABASE_URL')

@st.cache_resource
def init_connection():
    """Initialize database connection."""
    database_url = get_database_url()
    if not database_url:
        st.error("DATABASE_URL not found in secrets or environment variables")
        st.stop()
    return psycopg2.connect(database_url)

def get_conn():
    """Get database connection, reconnecting if necessary."""
    conn = init_connection()
    # Test if connection is alive by trying to get a cursor
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return conn
    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        # Connection is dead, clear cache and reconnect
        init_connection.clear()
        conn = init_connection()
        return conn

# Login
authenticator.login(location='main')

# Access authentication state from session state
name = st.session_state.get("name")
authentication_status = st.session_state.get("authentication_status")
username = st.session_state.get("username")

if authentication_status == False:
    st.error('Username/password is incorrect')
elif authentication_status == None:
    st.warning('Please enter your username and password')
elif authentication_status:
    # User is logged in
    st.session_state.user = username
    
    # Sidebar
    st.sidebar.title("⚽ NWSL Fantasy League")
    st.sidebar.write(f"**Logged in as:** {name}")
    authenticator.logout(location='sidebar')
    
    # Get user's leagues
    leagues_query = """
        SELECT DISTINCT l.league_id, l.league_name, t.team_name, t.team_id
        FROM nwsfl.leagues l
        JOIN nwsfl.teams t ON l.league_id = t.league_id
        WHERE t.user_id = %s
    """
    user_leagues = pd.read_sql(leagues_query, get_conn(), params=(username,))
    
    if user_leagues.empty:
        st.warning("You're not in any leagues yet! Contact the commissioner.")
        st.stop()
    
    # League selector
    league_options = {f"{row['league_name']} ({row['team_name']})": 
                     (row['league_id'], row['team_id']) 
                     for _, row in user_leagues.iterrows()}
    
    selected_league = st.sidebar.selectbox(
        "Select League",
        options=list(league_options.keys())
    )
    
    current_league_id, current_team_id = league_options[selected_league]
    
    # Navigation
    page = st.sidebar.radio("Menu", ["Standings", "My Roster", "Set Lineup", "Weekly Scores"])
    
    # PAGE: Standings
    if page == "Standings":
        st.title(f"🏆 Standings - {selected_league.split(' (')[0]}")
        
        query = """
            SELECT 
                t.team_name,
                t.user_id,
                COALESCE(ls.total_points, 0) as total_points
            FROM nwsfl.teams t
            LEFT JOIN nwsfl.league_standings ls ON t.team_id = ls.team_id
            WHERE t.league_id = %s
            ORDER BY total_points DESC
        """
        df = pd.read_sql(query, get_conn(), params=(current_league_id,))
        
        # Add rank
        df.insert(0, 'Rank', range(1, len(df) + 1))
        
        # Highlight current user's team
        def highlight_user(row):
            if row['user_id'] == username:
                return ['background-color: #e3f2fd'] * len(row)
            return [''] * len(row)
        
        st.dataframe(
            df.style.apply(highlight_user, axis=1),
            use_container_width=True,
            hide_index=True
        )
    
    # PAGE: My Roster
    elif page == "My Roster":
        st.title("📋 My Roster")
        
        query = """
            SELECT 
                p.player_name,
                p.position,
                p.team as nwsl_team,
                COALESCE(SUM(ps.points), 0) as total_points
            FROM nwsfl.rosters r
            JOIN nwsfl.players p ON r.player_id = p.player_id
            LEFT JOIN nwsfl.player_scores ps ON p.player_id = ps.player_id
            WHERE r.team_id = %s
            GROUP BY p.player_id, p.player_name, p.position, p.team
            ORDER BY p.position, total_points DESC
        """
        df = pd.read_sql(query, get_conn(), params=(current_team_id,))
        
        if df.empty:
            st.info("No players on your roster yet!")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
    
    # PAGE: Set Lineup
    elif page == "Set Lineup":
        st.title("⚡ Set Your Weekly Lineup")
        
        # Matchweek selector
        matchweek = st.number_input("Matchweek", min_value=1, max_value=30, value=1)
        
        # Get roster
        roster_query = """
            SELECT r.player_id, p.player_name, p.position, p.team
            FROM nwsfl.rosters r
            JOIN nwsfl.players p ON r.player_id = p.player_id
            WHERE r.team_id = %s
            ORDER BY p.position, p.player_name
        """
        roster = pd.read_sql(roster_query, get_conn(), params=(current_team_id,))
        
        if roster.empty:
            st.warning("You don't have any players on your roster!")
            st.stop()
        
        # Get current lineup
        lineup_query = """
            SELECT player_id 
            FROM nwsfl.lineups 
            WHERE team_id = %s AND matchweek = %s
        """
        current_lineup = pd.read_sql(lineup_query, get_conn(), params=(current_team_id, matchweek))
        
        # Create player display with position
        roster['display'] = roster['player_name'] + " (" + roster['position'] + " - " + roster['team'] + ")"
        
        # Multiselect for lineup
        default_selection = []
        if not current_lineup.empty:
            default_selection = roster[roster['player_id'].isin(current_lineup['player_id'])]['display'].tolist()
        
        selected_players = st.multiselect(
            "Select your starting lineup (choose 11 players)",
            options=roster['display'].tolist(),
            default=default_selection,
            help="Pick your starting 11 for this matchweek"
        )
        
        # Show selection count
        st.write(f"**Players selected:** {len(selected_players)}/11")
        
        col1, col2 = st.columns([1, 4])
        
        with col1:
            if st.button("💾 Save Lineup", type="primary", disabled=(len(selected_players) != 11)):
                try:
                    conn = get_conn()
                    cursor = conn.cursor()
                    
                    # Delete existing lineup
                    cursor.execute(
                        "DELETE FROM nwsfl.lineups WHERE team_id = %s AND matchweek = %s",
                        (current_team_id, matchweek)
                    )
                    
                    # Insert new lineup
                    for player_display in selected_players:
                        player_id = roster[roster['display'] == player_display]['player_id'].iloc[0]
                        cursor.execute(
                            "INSERT INTO nwsfl.lineups (team_id, matchweek, player_id) VALUES (%s, %s, %s)",
                            (current_team_id, matchweek, player_id)
                        )
                    
                    conn.commit()
                    cursor.close()
                    st.success(f"✅ Lineup saved for Matchweek {matchweek}!")
                except Exception as e:
                    st.error(f"Error saving lineup: {e}")
        
        with col2:
            if len(selected_players) != 11:
                st.warning("⚠️ You must select exactly 11 players")
    
    # PAGE: Weekly Scores
    elif page == "Weekly Scores":
        st.title("📊 Weekly Scores")
        
        matchweek = st.number_input("Select Matchweek", min_value=1, max_value=30, value=1)
        
        query = """
            SELECT 
                t.team_name,
                p.player_name,
                ps.points,
                ps.goals,
                ps.assists
            FROM nwsfl.lineups l
            JOIN nwsfl.teams t ON l.team_id = t.team_id
            JOIN nwsfl.players p ON l.player_id = p.player_id
            LEFT JOIN nwsfl.player_scores ps ON p.player_id = ps.player_id AND ps.matchweek = %s
            WHERE l.matchweek = %s AND t.league_id = %s
            ORDER BY t.team_name, ps.points DESC
        """
        df = pd.read_sql(query, get_conn(), params=(matchweek, matchweek, current_league_id))
        
        if df.empty:
            st.info(f"No scores available for Matchweek {matchweek} yet")
        else:
            # Group by team and show scores
            for team in df['team_name'].unique():
                team_df = df[df['team_name'] == team]
                total_points = team_df['points'].sum()
                
                with st.expander(f"**{team}** - {total_points} points"):
                    st.dataframe(
                        team_df[['player_name', 'points', 'goals', 'assists']],
                        use_container_width=True,
                        hide_index=True
                    )