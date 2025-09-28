import requests
import json

api_url = "https://engine.instabet.mx/api/game/getGamesByLeagueWithOdds"

# Headers from your network traffic
headers = {
    'authority': 'engine.instabet.mx',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json;charset=UTF-8',
    'epos-token': 'e5fd4c064115442d90e88ef7e33716d3',
    'languageid': '1',
    'origin': 'https://instabet.mx',
    'referer': 'https://instabet.mx/',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
}

payload = {'tournamentId': 7080,
           'isLive': False,
           'includeMarkets': True, 
           'includeOdds': True, 
           'marketIds': [1, 2, 3],
            'utc': '-6.0'}
'''
def format_odds(odds):
    """Format odds to 2 decimal places"""
    try:
        return f"{float(odds):.2f}"
    except (ValueError, TypeError):
        return str(odds)
        '''

try:
    response = requests.post(api_url, headers=headers, json=payload)
    
    if response.status_code == 200:
        data = response.json()
        
        if data.get('IsSuccess') and data.get('Data'):
            tournament_data = data['Data'][0]
            
            print(f"🎯 INSTABET LIGA MX ODDS SCRAPER - COMPLETE")
            print(f"🏆 {tournament_data.get('description', 'Tournament')}")
            print(f"📅 Scraped at: 2025-08-30 21:00:00")
            print("=" * 80)
            
            if 'dates' in tournament_data and tournament_data['dates']:
                total_games = 0
                
                for date_info in tournament_data['dates']:
                    date_str = date_info.get('date', 'Unknown date')
                    print(f"\n📅 {date_str}")
                    print("-" * 60)
                    
                    if 'games' in date_info and date_info['games']:
                        for game in date_info['games']:
                            total_games += 1
                            
                            # Extract game info
                            game_id = game.get('id', 'N/A')
                            game_time = game.get('gameDatetime', 'N/A')
                            home_team = game.get('homeTeam', 'Home')
                            away_team = game.get('visitorTeam', 'Away')
                            
                            print(f"⚽ {home_team} vs {away_team}")
                            print(f"   🕐 {game_time}")
                            
                            # Extract odds from GameMarketTypes
                            if 'GameMarketTypes' in game and game['GameMarketTypes']:
                                market_types = game['GameMarketTypes']
                                
                                # Money Line (1X2)
                                if 'moneyLine' in market_types and market_types['moneyLine']:
                                    money_line = market_types['moneyLine']
                                    if 'gameMarketOutComes' in money_line and money_line['gameMarketOutComes']:
                                        print("   💰 1X2 Odds:")
                                        for outcome in money_line['gameMarketOutComes']:
                                            outcome_type = outcome.get('outcomeType', 'N/A')
                                            outcome_desc = outcome.get('outcomeDescription', 'N/A')
                                            outcome_odd = outcome.get('odd', 'N/A')
                                            print(f"     {outcome_type}: {format_odds(outcome_odd)} ({outcome_desc})")
                                
                                # Spread (Handicap)
                                if 'spread' in market_types and market_types['spread']:
                                    spread = market_types['spread']
                                    if 'gameMarketOutComes' in spread and spread['gameMarketOutComes']:
                                        print("   💰 Handicap Odds:")
                                        for outcome in spread['gameMarketOutComes']:
                                            outcome_type = outcome.get('outcomeType', 'N/A')
                                            outcome_desc = outcome.get('outcomeDescription', 'N/A')
                                            outcome_odd = outcome.get('odd', 'N/A')
                                            print(f"     {outcome_type}: {format_odds(outcome_odd)} ({outcome_desc})")
                                
                                # Total (Over/Under)
                                if 'total' in market_types and market_types['total']:
                                    total = market_types['total']
                                    if 'gameMarketOutComes' in total and total['gameMarketOutComes']:
                                        print("   💰 Total Odds:")
                                        for outcome in total['gameMarketOutComes']:
                                            outcome_type = outcome.get('outcomeType', 'N/A')
                                            outcome_desc = outcome.get('outcomeDescription', 'N/A')
                                            outcome_odd = outcome.get('odd', 'N/A')
                                            print(f"     {outcome_type}: {format_odds(outcome_odd)} ({outcome_desc})")
                            
                            print()  # Empty line between games
                    
                print(f"\n📊 Successfully scraped {total_games} games")
                
            else:
                print("🚫 No games scheduled")
                
            # Create comprehensive data structure
            from datetime import datetime
            
            comprehensive_data = {
                'scraped_at': datetime.now().isoformat(),
                'api_url': api_url,
                'tournament': {
                    'id': tournament_data.get('tournamentId'),
                    'name': tournament_data.get('description'),
                    'sport': tournament_data.get('sport'),
                    'category': tournament_data.get('categoryDescription')
                },
                'games': []
            }
            
            if 'dates' in tournament_data and tournament_data['dates']:
                for date_info in tournament_data['dates']:
                    if 'games' in date_info and date_info['games']:
                        for game in date_info['games']:
                            game_data = {
                                'game_id': game.get('id'),
                                'datetime': game.get('gameDatetime'),
                                'home_team': game.get('homeTeam'),
                                'away_team': game.get('visitorTeam'),
                                'status': game.get('gameStatus', 'Not started'),
                                'odds': {}
                            }
                            
                            if 'GameMarketTypes' in game and game['GameMarketTypes']:
                                market_types = game['GameMarketTypes']
                                
                                for market_name in ['moneyLine', 'spread', 'total']:
                                    if market_name in market_types:
                                        market_data = market_types[market_name]
                                        if market_data and 'gameMarketOutComes' in market_data and market_data['gameMarketOutComes']:
                                            game_data['odds'][market_name] = []
                                            for outcome in market_data['gameMarketOutComes']:
                                                if outcome:  # Check if outcome is not None
                                                    game_data['odds'][market_name].append({
                                                        'type': outcome.get('outcomeType'),
                                                        'description': outcome.get('outcomeDescription'),
                                                        'odds': outcome.get('odd'),
                                                        'point': outcome.get('point'),
                                                        'outcome_id': outcome.get('outcomeId')
                                                    })
                            
                            comprehensive_data['games'].append(game_data)
            
            # Save the comprehensive data
            with open('instabet_comprehensive_data.json', 'w', encoding='utf-8') as f:
                json.dump(comprehensive_data, f, indent=2, ensure_ascii=False)
            print(f"💾 Comprehensive data saved to 'instabet_comprehensive_data.json'")
            
            # Also save a simplified CSV version
            import csv
            
            with open('instabet_odds_simple.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Date', 'Time', 'Home Team', 'Away Team', 'Market', 'Outcome', 'Odds', 'Point'])
                
                for game in comprehensive_data['games']:
                    date_time = game.get('datetime', '')
                    game_date = date_time.split('T')[0] if date_time else ''
                    game_time = date_time.split('T')[1] if date_time and 'T' in date_time else ''
                    
                    for market_name, outcomes in game.get('odds', {}).items():
                        for outcome in outcomes:
                            writer.writerow([
                                game_date,
                                game_time,
                                game.get('home_team', ''),
                                game.get('away_team', ''),
                                market_name,
                                outcome.get('description', ''),
                                outcome.get('odds', ''),
                                outcome.get('point', '')
                            ])
            
            print(f"📄 Simple CSV data saved to 'instabet_odds_simple.csv'")
            print(f"\n🎉 SCRAPING COMPLETE!")
            print(f"✅ API endpoint: {api_url}")
            print(f"✅ Headers configured correctly")
            print(f"✅ Payload: {enhanced_payload}")
            print(f"✅ Data extracted and saved in multiple formats")
                
        else:
            print("❌ API call failed")
            print(f"Error: {data.get('ExceptionMessage', 'Unknown error')}")
            
    else:
        print(f"❌ HTTP Error: {response.status_code}")
        print(f"Response: {response.text[:200]}...")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()