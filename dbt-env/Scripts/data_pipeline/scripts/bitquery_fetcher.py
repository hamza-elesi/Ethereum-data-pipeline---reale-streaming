import http.client
import json
import pandas as pd
from datetime import datetime

def fetch_ethereum_data(start_date, end_date, limit=100):
    conn = http.client.HTTPSConnection("graphql.bitquery.io")
    
    query = """
    query ($network: EthereumNetwork!, $limit: Int!, $offset: Int!, $from: ISO8601DateTime, $till: ISO8601DateTime) {
      ethereum(network: $network) {
        transfers(
          options: {desc: "amount", limit: $limit, offset: $offset}
          amount: {gt: 0}
          time: {since: $from, till: $till}
        ) {
          currency {
            symbol
            address
          }
          count
          senders: count(uniq: senders)
          receivers: count(uniq: receivers)
          amount
          amount_usd: amount(in: USD)
          median: amount(calculate: median)
          maximum: amount(calculate: maximum)
        }
        dexTrades(
          options: {desc: "tradeAmount", limit: 10}
          time: {since: $from, till: $till}
        ) {
          exchange {
            name
          }
          tradeAmount(in: USD)
        }
      }
    }
    """
    
    variables = {
        "limit": limit,
        "offset": 0,
        "network": "ethereum",
        "from": start_date.isoformat(),
        "till": end_date.isoformat(),
        "dateFormat": "%Y-%m-%d"
    }
    
    payload = json.dumps({
        "query": query,
        "variables": json.dumps(variables)
    })
    
    headers = {
        'Content-Type': 'application/json',
        'X-API-KEY': 'BQYspPqA4o9aGIAnvZjqxPTSqjotSWN9'
    }
    
    conn.request("POST", "/", payload, headers)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode("utf-8"))


start_date = datetime(2024, 11, 3)
end_date = datetime(2024, 11, 4)

data = fetch_ethereum_data(start_date, end_date)
# save the data to a file for later use
with open("ethereum_data_final.json", "w") as f:
    f.write(json.dumps(data, indent=4))
    
#print(json.dumps(data, indent=4))