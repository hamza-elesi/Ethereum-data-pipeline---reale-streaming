# scripts/run_pipeline.py
import subprocess
from datetime import datetime, timedelta
from bitquery_fetcher import fetch_ethereum_data
from snowflake_loader import load_to_snowflake

def run_pipeline():
    try:
        print("1. Fetching data from BitQuery...")
        data = fetch_ethereum_data(
            start_date=datetime.now() - timedelta(days=1),
            end_date=datetime.now()
        )
        
        print("2. Loading data to Snowflake...")
        load_to_snowflake(data)
        
        print("3. Running dbt...")
        subprocess.run(["dbt", "deps"], check=True)
        subprocess.run(["dbt", "run"], check=True)
        subprocess.run(["dbt", "test", "--warn-error"], check=True)
        
        print("Pipeline completed successfully!")
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise e

if __name__ == "__main__":
    run_pipeline()