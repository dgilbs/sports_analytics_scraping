# NWSL Fantasy League App

A Streamlit-based fantasy league application for the NWSL.

## Setup

1. Clone the repository
2. Install dependencies:
```bash
   pip install -r requirements.txt
```

3. Create `.env` file with your database URL:
```
   DATABASE_URL=postgresql://user:password@host:port/database
```

4. Generate password hashes:
```bash
   python scripts/generate_passwords.py
```

5. Fill in `config.yaml` with user credentials (use generated hashes)

6. Run the app:
```bash
   streamlit run nwsfl_app.py
```

## Database Schema

The app expects the following tables:
- `leagues` - League information
- `teams` - Teams within leagues (links users to leagues)
- `rosters` - Player rosters for each team
- `lineups` - Weekly lineups
- `players` - NWSL player information
- `player_scores` - Weekly player scores
- `league_standings` - Current standings

## Deployment

Deploy to Streamlit Cloud:
1. Push code to GitHub
2. Connect repository at [share.streamlit.io](https://share.streamlit.io)
3. Add secrets in Streamlit Cloud settings (same format as `config.yaml`)
4. Deploy!

## Security Note

**Never commit:**
- `.env` file
- `config.yaml` with real passwords
- Any file containing database credentials