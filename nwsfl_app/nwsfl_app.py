import streamlit as st
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import yaml
from yaml.loader import SafeLoader
import bcrypt

# Load .env file if it exists, but don't fail if it doesn't
try:
    load_dotenv()
except:
    pass

# Page config
st.set_page_config(page_title="NWSFL", page_icon="⚽", layout="wide")

# Load authentication config
if os.path.exists('config.yaml'):
    with open('config.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)
else:
    st.error("config.yaml not found")
    st.stop()

# Custom authentication function
def verify_password(password: str, stored_password: str) -> bool:
    """Verify a plain text password"""
    # Simple string comparison - passwords are stored as plain text in config
    return password == stored_password

# Initialize session state for auth
if 'authentication_status' not in st.session_state:
    st.session_state.authentication_status = None
    st.session_state.username = None
    st.session_state.name = None

# Login UI
if st.session_state.authentication_status is None or st.session_state.authentication_status == False:
    st.title("⚽ NWSL Fantasy League Login")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        
        if st.button("Login", type="primary", use_container_width=True):
            # Check if user exists
            if username in config['credentials']['usernames']:
                user_data = config['credentials']['usernames'][username]
                stored_hash = user_data.get('password', '')
                
                # Verify password
                if verify_password(password, stored_hash):
                    st.session_state.authentication_status = True
                    st.session_state.username = username
                    st.session_state.name = user_data.get('name', username)
                    st.success("✅ Login successful!")
                    st.rerun()
                else:
                    st.error("❌ Username/password is incorrect")
            else:
                st.error("❌ Username/password is incorrect")
else:
    # User is logged in
    st.session_state.user = st.session_state.username
    name = st.session_state.name
    username = st.session_state.username
    
    # Database connection
    def get_database_url():
        """Get the database URL from environment or secrets."""
        # Try environment variable first
        env_url = os.getenv('DATABASE_URL')
        if env_url:
            return env_url
        
        # Try secrets
        try:
            if "DATABASE_URL" in st.secrets:
                return st.secrets["DATABASE_URL"]
            if "database" in st.secrets and "url" in st.secrets["database"]:
                return st.secrets["database"]["url"]
        except:
            pass
        
        # Fallback (for development only)
        return "postgresql://neondb_owner:npg_RSU6cfsvr8zy@ep-round-boat-aeeid91z-pooler.c-2.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

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
    
    # Sidebar
    st.sidebar.title("⚽ NWSL Fantasy League")
    st.sidebar.write(f"**Logged in as:** {name}")
    
    if st.sidebar.button("🚪 Logout", type="secondary"):
        st.session_state.authentication_status = None
        st.session_state.username = None
        st.session_state.name = None
        st.rerun()
    
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
    page = st.sidebar.radio("Menu", ["Standings", "My Roster", "Player Market", "Set Lineup", "Weekly Scores"])
    
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
                player,
                fantasy_position as position
            FROM season_fantasy_rosters
            WHERE nwsfl_team_id = %s
            ORDER BY fantasy_position, player
        """
        df = pd.read_sql(query, get_conn(), params=(current_team_id,))
        
        if df.empty:
            st.info("No players on your roster yet!")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
    
    # PAGE: Player Market
    elif page == "Player Market":
        st.title("📊 Player Market")
        st.write("View all available players and their current ownership status")
        
        # Get all players with their ownership status for this league
        # Using nwsfl_fantasy_players_2025 joined with season_fantasy_rosters to show ownership
        query = """
            SELECT DISTINCT
                nfp.player_id,
                nfp.player,
                nfp.fantasy_position,
                nfp.squad as nwsl_team,
                COALESCE(ft.team_name, 'Free Agent') as fantasy_team,
                CASE 
                    WHEN sfr.player_id IS NOT NULL THEN 'Owned'
                    ELSE 'Free Agent'
                END as status
            FROM soccer.nwsfl_fantasy_players_2025 nfp
            LEFT JOIN season_fantasy_rosters sfr ON nfp.player_id = sfr.player_id
            LEFT JOIN fantasy_teams ft ON sfr.nwsfl_team_id = ft.id AND ft.league_id = %s
            ORDER BY nfp.fantasy_position, nfp.player
        """
        df = pd.read_sql(query, get_conn(), params=(current_league_id,))
        
        if df.empty:
            st.info("No players available")
        else:
            # Add filtering options
            col1, col2, col3 = st.columns(3)
            
            with col1:
                position_filter = st.multiselect(
                    "Filter by Position",
                    options=sorted([p for p in df['fantasy_position'].unique() if p is not None]),
                    default=sorted([p for p in df['fantasy_position'].unique() if p is not None])
                )
            
            with col2:
                status_filter = st.multiselect(
                    "Filter by Status",
                    options=['Owned', 'Free Agent'],
                    default=['Owned', 'Free Agent']
                )
            
            with col3:
                nwsl_team_filter = st.multiselect(
                    "Filter by NWSL Team",
                    options=sorted(df['nwsl_team'].unique()),
                    default=sorted(df['nwsl_team'].unique())
                )
            
            # Apply filters
            filtered_df = df[
                (df['fantasy_position'].isin(position_filter)) &
                (df['status'].isin(status_filter)) &
                (df['nwsl_team'].isin(nwsl_team_filter))
            ]
            
            # Display stats
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Players", len(df))
            col2.metric("Owned", len(df[df['status'] == 'Owned']))
            col3.metric("Free Agents", len(df[df['status'] == 'Free Agent']))
            col4.metric("Showing", len(filtered_df))
            
            st.divider()
            
            # Display dataframe with styling
            def highlight_ownership(row):
                if row['Status'] == 'Owned':
                    return ['background-color: #e8f5e9'] * len(row)  # Light green for owned
                else:
                    return ['background-color: #fff3e0'] * len(row)  # Light orange for free agents
            
            # Select columns to display
            display_df = filtered_df[['player', 'fantasy_position', 'nwsl_team', 'fantasy_team', 'status']].copy()
            display_df.columns = ['Player Name', 'Position', 'NWSL Team', 'Fantasy Team', 'Status']
            
            st.dataframe(
                display_df.style.apply(highlight_ownership, axis=1),
                use_container_width=True,
                hide_index=True
            )
    
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