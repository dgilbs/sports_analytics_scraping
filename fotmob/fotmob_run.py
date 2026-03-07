import asyncio
from scrape_match_reports import scrape_match_player_data

url = 'https://www.fotmob.com/matches/gotham-fc-vs-washington-spirit/2gzkzwps#5039042:tab=stats'

if __name__ == "__main__":
    asyncio.run(scrape_match_player_data(url))


