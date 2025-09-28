#!/usr/bin/env python3
"""
Arbitrage Bot Scheduler - Runs the arbitrage detection every 10 minutes
"""

import time
import schedule
import logging
from datetime import datetime
from arbitrage_bot import detect_arbitrage_opportunities, create_unified_odds_framework
from new_scraper import get_instabet_df
from pinnacle_odds import get_pinnacle_df
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/arbitrage_scheduler.log'),
        logging.StreamHandler()
    ]
)

def run_arbitrage_check():
    """Run a complete arbitrage check cycle"""
    try:
        logging.info("Starting arbitrage check...")
        
        # Get data from both sources
        df_instabet = get_instabet_df()
        df_odds_api = get_pinnacle_df()
        
        if df_instabet.empty or df_odds_api.empty:
            logging.warning("One or both data sources returned empty data")
            return
        
        # Create unified framework
        unified_df = create_unified_odds_framework(df_instabet, df_odds_api)
        
        # Detect arbitrage opportunities
        arbitrage_ops = detect_arbitrage_opportunities(unified_df, total_stake=1000)
        
        if arbitrage_ops.empty:
            logging.info("No arbitrage opportunities found")
        else:
            logging.info(f"Found {len(arbitrage_ops)} arbitrage opportunities!")
            
            # Save results to CSV
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"arbitrage_results/arbitrage_ops_{timestamp}.csv"
            arbitrage_ops.to_csv(filename, index=False)
            logging.info(f"Results saved to {filename}")
            
            # Print summary
            for match_id in arbitrage_ops['match_id'].unique():
                match_ops = arbitrage_ops[arbitrage_ops['match_id'] == match_id]
                profit_pct = match_ops['profit_percentage'].iloc[0]
                logging.info(f"Arbitrage: {match_id} - {profit_pct:.2f}% profit")
    
    except Exception as e:
        logging.error(f"Error during arbitrage check: {e}")
        import traceback
        logging.error(traceback.format_exc())

def main():
    """Main scheduler function"""
    logging.info("Arbitrage Bot Scheduler started")
    
    # Create directories if they don't exist
    import os
    os.makedirs("logs", exist_ok=True)
    os.makedirs("arbitrage_results", exist_ok=True)
    
    # Schedule to run every 10 minutes
    schedule.every(10).minutes.do(run_arbitrage_check)
    
    # Run immediately on startup
    run_arbitrage_check()
    
    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()