#!/usr/bin/env python
# coding: utf-8

# In[2]:


import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage

get_ipython().system('gcloud config set project striking-ruler-461123-e3')
get_ipython().system('gcloud auth application-default login')
get_ipython().system('pip install google-cloud-storage')

# In[3]:


# === CONFIG ===
ticker = "AAPL"
bucket_name = "stock-movements-bucket"
gcs_prefix = f"{ticker}/"

# === Get Yesterday’s Date ===
yesterday = datetime.today() - timedelta(days=1)
start_date = yesterday.strftime("%Y-%m-%d")
end_date = (yesterday + timedelta(days=1)).strftime("%Y-%m-%d")


max_lookback = 5
for offset in range(1, max_lookback + 1):
    date = datetime.today() - timedelta(days=offset)
    start_date = date.strftime("%Y-%m-%d")
    end_date = (date + timedelta(days=1)).strftime("%Y-%m-%d")

    df = yf.download(ticker, start=start_date, end=end_date)
    if not df.empty:
        print(f"Data fetched for {start_date}")
        break
else:
    print("No data found for past few days. Exiting.")
    exit()


df = yf.download(
    tickers=ticker,
    period="1d",            # fetches today’s data (if market is open)
    interval="1m",          # 1-minute granularity
    progress=False,
    threads=True,
)

date_str = datetime.now().strftime("%Y-%m-%d")
file_name = f"{ticker}_1min_{date_str}.csv"
df.reset_index().to_csv(file_name, index=False)
print(f"Saved: {file_name}")


client = storage.Client()
bucket = client.bucket(bucket_name)
blob = bucket.blob(f"{gcs_prefix}intraday/{file_name}")
blob.upload_from_filename(file_name)
print(f"Uploaded to gs://{bucket_name}/{gcs_prefix}intraday/{file_name}")