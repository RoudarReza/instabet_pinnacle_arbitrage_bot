import pandas as pd
import numpy as np
import requests
import json
import time
import pytz
from dateutil.parser import parse
from scipy.optimize import minimize

def get_pinnacle_df() -> pd.DataFrame:
    """
    Scrape Pinnacle odds data from The Odds API and return as a DataFrame
    
    Returns:
        pd.DataFrame: DataFrame containing Pinnacle 1X2 odds data
    """
    API_KEY = '4bfd6fe2cb5da3be05d7aac7bbcc9b44'
    SPORT = 'soccer_mexico_ligamx'
    REGIONS = 'eu'
    MARKETS = 'h2h'

    url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds"
    params = {
        "apiKey": API_KEY,
        "regions": REGIONS,
        "markets": MARKETS,
        "oddsFormat": "decimal"  # or "american"
    }

    try:
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            print(f"Error: HTTP {response.status_code}")
            return pd.DataFrame()
            
        odds_data = response.json()

        # ---- flatten odds_data → rows_list ----
        rows_list = []

        # sanity check
        if isinstance(odds_data, list) and all(isinstance(it, dict) for it in odds_data):
            for game in odds_data:    # each match
                for bookmaker in game.get('bookmakers', []):    # each bookie for the match
                    for market_ in bookmaker.get('markets', []):  # each market offered by the bookie
                        for outcome in market_.get('outcomes', []):   # each outcome in the market
                            rows_list.append({
                                # game-level
                                'game_id': game.get('id'),
                                'sport_key': game.get('sport_key'),
                                'sport_title': game.get('sport_title'),
                                'commence_time': game.get('commence_time'),
                                'home_team': game.get('home_team'),
                                'away_team': game.get('away_team'),

                                # bookmaker-level
                                'bookmaker_key': bookmaker.get('key'),
                                'bookmaker_title': bookmaker.get('title'),
                                'bookmaker_last_update': bookmaker.get('last_update'),

                                # market-level
                                'market_key': market_.get('key'),
                                'market_last_update': market_.get('last_update'),

                                # outcome-level
                                'outcome_name': outcome.get('name'),   # e.g., 'Necaxa', 'FC Juárez', 'Draw'
                                'outcome_price': outcome.get('price')   # decimal odds
                            })

        # rows → DataFrame
        df = pd.DataFrame(rows_list)
        
        # Filter for Pinnacle and reasonable odds
        pinnacle_1x2 = df[df['bookmaker_key'] == 'pinnacle']
        pinnacle_1x2 = pinnacle_1x2[pinnacle_1x2['outcome_price'] < 20]
        
        return pinnacle_1x2
        
    except Exception as e:
        print(f"Error fetching Pinnacle odds: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
        # Keep script runnable from CLI
    pinnacle_1x2 = get_pinnacle_df()