# scripts/snowflake_loader.py
import snowflake.connector
from datetime import datetime, timedelta
from bitquery_fetcher import fetch_ethereum_data

def load_to_snowflake(data):
    # Snowflake connection using your DBT_DB and DBT_SCHEMA
    conn = snowflake.connector.connect(
        user='INKOHAMZA',                # Changed to uppercase to match Snowflake
        password='SH210918elmaataoui',
        account='pj10438.uk-south.azure', # Removed https:// and .snowflakecomputing.com
        warehouse='DBT_WH',              # Changed to uppercase
        database='DBT_DB',               # Changed to uppercase
        schema='DBT_SCHEMA',             # Changed to uppercase
        role='ACCOUNTADMIN'              # Added role parameter
    )
    
    cursor = conn.cursor()
    
    try:
        # Create database and schema if they don't exist
        cursor.execute("CREATE DATABASE IF NOT EXISTS DBT_DB")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS DBT_DB.DBT_SCHEMA")
        
        # Create raw tables
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS DBT_DB.DBT_SCHEMA.RAW_TRANSFERS (
            token_symbol STRING,
            token_address STRING,
            transaction_count INTEGER,
            unique_senders INTEGER,
            unique_receivers INTEGER,
            amount FLOAT,
            amount_usd FLOAT,
            median_amount FLOAT,
            max_amount FLOAT,
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS DBT_DB.DBT_SCHEMA.RAW_DEX_TRADES (
            exchange_name STRING,
            trade_amount_usd FLOAT,
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        
        # Insert data
        if 'data' in data and 'ethereum' in data['data']:
            # Load transfers
            if 'transfers' in data['data']['ethereum']:
                for transfer in data['data']['ethereum']['transfers']:
                    cursor.execute("""
                    INSERT INTO DBT_DB.DBT_SCHEMA.RAW_TRANSFERS (
                        token_symbol, token_address, transaction_count, 
                        unique_senders, unique_receivers, amount, 
                        amount_usd, median_amount, max_amount
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        transfer['currency']['symbol'],
                        transfer['currency']['address'],
                        transfer['count'],
                        transfer['senders'],
                        transfer['receivers'],
                        transfer['amount'],
                        transfer['amount_usd'],
                        transfer['median'],
                        transfer['maximum']
                    ))
            
            # Load dex trades
            if 'dexTrades' in data['data']['ethereum']:
                for trade in data['data']['ethereum']['dexTrades']:
                    cursor.execute("""
                    INSERT INTO DBT_DB.DBT_SCHEMA.RAW_DEX_TRADES (
                        exchange_name, trade_amount_usd
                    ) VALUES (%s, %s)
                    """, (
                        trade['exchange']['name'],
                        trade['tradeAmount']
                    ))
            
            conn.commit()
            print("Data loaded successfully!")
        else:
            print("No data found in the expected format")
            
    except Exception as e:
        print(f"Error loading data: {e}")
        conn.rollback()
    
    finally:
        cursor.close()
        conn.close()

def verify_data_load():
    """Verify that data was loaded correctly"""
    conn = snowflake.connector.connect(
        user='INKOHAMZA',
        password='SH210918elmaataoui',
        account='pj10438.uk-south.azure',
        warehouse='DBT_WH',
        database='DBT_DB',
        schema='DBT_SCHEMA',
        role='ACCOUNTADMIN'
    )
    
    cursor = conn.cursor()
    
    try:
        # Check record counts
        cursor.execute("SELECT COUNT(*) FROM DBT_DB.DBT_SCHEMA.RAW_TRANSFERS")
        transfers_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM DBT_DB.DBT_SCHEMA.RAW_DEX_TRADES")
        trades_count = cursor.fetchone()[0]
        
        print(f"Verification Results:")
        print(f"- Transfers loaded: {transfers_count}")
        print(f"- DEX trades loaded: {trades_count}")
        
    except Exception as e:
        print(f"Error verifying data: {e}")
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Fetch and load yesterday's data
    start_date = datetime.now() - timedelta(days=1)
    end_date = datetime.now()
    
    print(f"Fetching data from {start_date} to {end_date}")
    data = fetch_ethereum_data(start_date, end_date)
    
    print("Loading data to Snowflake...")
    load_to_snowflake(data)
    
    print("Verifying data load...")
    verify_data_load()