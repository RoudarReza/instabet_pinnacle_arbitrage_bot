from new_scraper import get_instabet_df
from pinnacle_odds import get_pinnacle_df
import pandas as pd
import numpy as np
import requests
import json
import time 
import pytz
from dateutil.parser import parse
from scipy.optimize import minimize

# Get the data from both sources
df_instabet = get_instabet_df()  # Instabet web scraped data
df_odds_api = get_pinnacle_df()  # Pinnacle data from Odds API

# Create a unified data structure framework
def create_unified_odds_framework(df_instabet, df_odds_api):
    """
    Create a unified framework for both Instabet and Odds API data
    Returns a standardized dataframe with consistent structure
    """
    
    # Standardize team names across both datasets - COMPLETE MAPPING
    team_mapping = {
        # Instabet to Standard names
        'Club Necaxa': 'Necaxa',
        'FC Juarez': 'FC Juárez',
        'Mazatlan FC': 'Mazatlán FC',
        'Pumas UNAM': 'Pumas',
        'CF Pachuca': 'Pachuca',
        'CF Cruz Azul': 'Cruz Azul',
        'Tigres UANL': 'Tigres',
        'Club León': 'León',
        'Atlas FC': 'Atlas',
        'Deportivo Toluca FC': 'Toluca',
        'CF América': 'América',
        'Querétaro FC': 'Querétaro',
        'Atlético San Luís': 'Atlético San Luis',
        'Club Tijuana': 'Tijuana',
        'Chivas de Guadalajara': 'Guadalajara',
        'CF Monterrey': 'Monterrey',
        'Club Santos Laguna': 'Santos Laguna',
        'Club Puebla': 'Puebla',
        
        # Odds API to Standard names (some might already be standard)
        'Necaxa': 'Necaxa',
        'Mazatlán FC': 'Mazatlán FC',
        'Pachuca': 'Pachuca',
        'Atlas': 'Atlas',
        'Tigres': 'Tigres',
        'Toluca': 'Toluca',
        'América': 'América',
        'Querétaro': 'Querétaro',
        'Atlético San Luis': 'Atlético San Luis',
        'FC Juárez': 'FC Juárez',
        'Pumas': 'Pumas',
        'Cruz Azul': 'Cruz Azul',
        'Santos Laguna': 'Santos Laguna',
        'León': 'León',
        'Puebla': 'Puebla',
        'Guadalajara': 'Guadalajara',
        'Monterrey': 'Monterrey',
        'Tijuana': 'Tijuana'
    }
    
    # Clean and standardize Instabet data
    df_instabet_clean = df_instabet.copy()
    df_instabet_clean['home_team'] = df_instabet_clean['home_team'].replace(team_mapping)
    df_instabet_clean['away_team'] = df_instabet_clean['away_team'].replace(team_mapping)
    
    # Clean and standardize Odds API data
    df_odds_api_clean = df_odds_api.copy()
    df_odds_api_clean['home_team'] = df_odds_api_clean['home_team'].replace(team_mapping)
    df_odds_api_clean['away_team'] = df_odds_api_clean['away_team'].replace(team_mapping)
    
    # Standardize outcome types for Instabet
    outcome_mapping = {'home': 'home_win', 'visitor': 'away_win', 'draw': 'draw'}
    df_instabet_clean['outcome_type'] = df_instabet_clean['outcome_type'].replace(outcome_mapping)
    
    # Create unique match identifier
    df_instabet_clean['match_id'] = df_instabet_clean['home_team'] + ' vs ' + df_instabet_clean['away_team']
    df_odds_api_clean['match_id'] = df_odds_api_clean['home_team'] + ' vs ' + df_odds_api_clean['away_team']
    
    # Standardize datetime format
    df_instabet_clean['datetime'] = pd.to_datetime(df_instabet_clean['datetime'])
    df_odds_api_clean['commence_time'] = pd.to_datetime(df_odds_api_clean['commence_time'])
    
    # Create unified dataframe structure
    unified_data = []
    
    # Process Instabet data
    for _, row in df_instabet_clean.iterrows():
        unified_data.append({
            'source': 'instabet',
            'match_id': row['match_id'],
            'datetime': row['datetime'],
            'home_team': row['home_team'],
            'away_team': row['away_team'],
            'outcome_type': row['outcome_type'],
            'odds': row['odds'],
            'bookmaker': 'Instabet',
            'market_type': '1x2',
            'sport': 'soccer',
            'league': 'Liga MX',
            'update_time': pd.Timestamp.now()
        })

    # Process Odds API data
        outcome_name_mapping = {
        # Map outcome names to standardized outcome types
        # The Odds API returns team names as outcome names, so we need to determine
        # whether each outcome represents home_win, away_win, or draw
        'Draw': 'draw'
        }

    for _, row in df_odds_api_clean.iterrows():
        # Determine outcome type based on team name
        outcome_type = None
    if row['outcome_name'] == 'Draw':
        outcome_type = 'draw'
    elif row['outcome_name'] == row['home_team']:
        outcome_type = 'home_win'
    elif row['outcome_name'] == row['away_team']:
        outcome_type = 'away_win'

    if outcome_type:
        unified_data.append({
        'source': 'odds_api',
        'match_id': row['match_id'],
        'datetime': row['commence_time'],
        'home_team': row['home_team'],
        'away_team': row['away_team'],
        'outcome_type': outcome_type,
        'odds': row['outcome_price'],
        'bookmaker': row['bookmaker_title'],
        'market_type': '1x2',
        'sport': 'soccer',
        'league': 'Liga MX',
        'update_time': pd.to_datetime(row['bookmaker_last_update'])
        })
    
    return pd.DataFrame(unified_data)

unified_df = create_unified_odds_framework(df_instabet, df_odds_api)

def detect_arbitrage_opportunities(unified_df, total_stake=1000):
    """
    Detect arbitrage opportunities from unified odds dataframe
    
    Parameters:
    unified_df: DataFrame with columns: match_id, outcome_type, odds, bookmaker
    total_stake: Total amount to invest
    
    Returns:
    DataFrame with arbitrage opportunities and calculated stakes
    """
    
    # Check if we have data to process
    if unified_df.empty:
        print("DataFrame is empty - no data to process")
        return pd.DataFrame()
    
    # Verify required columns exist
    required_columns = ['match_id', 'outcome_type', 'odds', 'bookmaker']
    missing_columns = [col for col in required_columns if col not in unified_df.columns]
    if missing_columns:
        print(f"Missing required columns: {missing_columns}")
        return pd.DataFrame()
    
    # Convert odds to numeric (handle any string values)
    unified_df = unified_df.copy()
    unified_df['odds'] = pd.to_numeric(unified_df['odds'], errors='coerce')
    
    # Remove rows with invalid odds
    df_clean = unified_df.dropna(subset=['odds'])
    
    if df_clean.empty:
        print("No valid odds data after cleaning")
        return pd.DataFrame()

    # Group by match and outcome, then find max odds
    max_odds_idx = df_clean.groupby(['match_id', 'outcome_type'])['odds'].idxmax()
    
    # Get the rows with maximum odds
    df_max_odds = df_clean.loc[max_odds_idx].copy()
    
    # Calculate implied probabilities
    df_max_odds['implied_probability'] = 1 / df_max_odds['odds']
    
    # Calculate sum of implied probabilities for each match
    sum_probs = df_max_odds.groupby('match_id')['implied_probability'].sum()
    
    # Add sum probabilities to each row
    df_max_odds['sum_implied_prob'] = df_max_odds['match_id'].map(sum_probs)
    
    # Filter for arbitrage opportunities (sum < 1)
    df_arbitrage = df_max_odds[df_max_odds['sum_implied_prob'] <= 1.05].copy()
    
    if df_arbitrage.empty:
        print("No arbitrage opportunities found (all sum_implied_prob >= 1)")
        return pd.DataFrame()
    
    # Check if we have complete sets of outcomes (all 3 for 1x2 market)
    outcome_counts = df_arbitrage.groupby('match_id')['outcome_type'].count()
    complete_matches = outcome_counts[outcome_counts == 3].index
    
    if len(complete_matches) == 0:
        print("No complete arbitrage opportunities (missing outcomes)")
        return pd.DataFrame()
    
    # Filter for complete arbitrage opportunities
    df_complete_arbitrage = df_arbitrage[df_arbitrage['match_id'].isin(complete_matches)]
    
    # Calculate optimal stakes
    df_complete_arbitrage['stake'] = (
        total_stake * df_complete_arbitrage['implied_probability'] / 
        df_complete_arbitrage['sum_implied_prob']
    )
    
    # Calculate returns
    df_complete_arbitrage['return'] = df_complete_arbitrage['stake'] * df_complete_arbitrage['odds']
    
    # Calculate profit percentage
    df_complete_arbitrage['profit_percentage'] = (
        (1 / df_complete_arbitrage['sum_implied_prob'] - 1) * 100
    )
    
    # Add total stake and expected profit
    df_complete_arbitrage['total_stake'] = total_stake
    df_complete_arbitrage['expected_profit'] = (
        df_complete_arbitrage['return'] - df_complete_arbitrage['stake']
    )
    
    return df_complete_arbitrage


def create_message(game_df):
    """
    Create a formatted message for arbitrage opportunities
    
    Parameters:
    game_df: DataFrame with arbitrage data for a single match
    
    Returns:
    str: Formatted message string
    """
    # Get the first row from the game DataFrame
    row = game_df.iloc[0]

    # Parse the datetime and convert to Mexico City timezone
    datetime_obj = pd.to_datetime(row['datetime'])
    datetime_obj = datetime_obj.tz_convert('America/Mexico_City')
    datetime_str = datetime_obj.strftime('%d/%m/%Y %A %H:%M (GMT%z)')

    # Extract sport and team names
    sport = row['sport']
    home_team = row['home_team']
    away_team = row['away_team']

    # Create a dictionary of outcomes
    outcome_dict = game_df.set_index('outcome_type').to_dict(orient='index')

    # Order the outcomes based on home_win, draw, and away_win
    ordered_outcomes = ['home_win', 'draw', 'away_win']
    outcome_labels = {
        'home_win': home_team,
        'draw': 'Draw',
        'away_win': away_team
    }

    # Initialize the message string
    message = f"""
{home_team} vs {away_team} ({sport})

{datetime_str}

Odds:"""

    # Add odds to the message
    for outcome in ordered_outcomes:
        outcome_row = outcome_dict.get(outcome)
        if outcome_row:
            message += f"""
{outcome_row['bookmaker']}: {outcome_row['odds']:.2f}"""

    # Add stakes to the message
    message += f"""

   Stakes:"""
    for outcome in ordered_outcomes:
        outcome_row = outcome_dict.get(outcome)
        if outcome_row:
            stake_rounded = round(outcome_row['stake'], 2)
            message += f"""
{outcome_labels[outcome]}: {stake_rounded}"""

    # Add rate of return (ROR) to the message
    message += f"""

   ROR:"""
    for outcome in ordered_outcomes:
        outcome_row = outcome_dict.get(outcome)
        if outcome_row:
            ror_rounded = round(outcome_row['profit_percentage'], 2)
            message += f"""
{outcome_labels[outcome]}: {ror_rounded} %"""

    # Add total information
    message += f"""

   Total Stake: {row['total_stake']}
   Expected Profit: {round(row['expected_profit'], 2)}
   Overall Profit: {row['profit_percentage']:.2f} %"""

    return message


def send_arbitrage_alerts(arbitrage_ops):
    """
    Send alerts for arbitrage opportunities
    
    Parameters:
    arbitrage_ops: DataFrame with arbitrage opportunities
    """
    if arbitrage_ops.empty:
        print("No arbitrage opportunities to alert")
        return
    
    # Group by match and send alerts
    for match_id in arbitrage_ops['match_id'].unique():
        match_ops = arbitrage_ops[arbitrage_ops['match_id'] == match_id]
        message = create_message(match_ops)
        print(f"\n=== ARBITRAGE ALERT ===")
        print(message)
        print("=" * 40)
        
        # Here you could add email/SMS/telegram integration
        # For now, we just print to console

# Example usage:
if __name__ == "__main__":
    # Assuming you have your unified_df from previous steps
    # unified_df = your_unified_dataframe
    
    # Detect arbitrage opportunities
    arbitrage_opportunities = detect_arbitrage_opportunities(unified_df, total_stake=1000)

print(arbitrage_opportunities)