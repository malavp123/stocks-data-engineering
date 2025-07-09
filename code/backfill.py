#!/usr/bin/env python
# coding: utf-8

# In[1]:


import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage
import os


# === CONFIG ===
ticker = "AAPL"
bucket_name = "stock-movements-bucket"
gcs_prefix = f"{ticker}/intraday/"


get_ipython().system('pip install yfinance google-cloud-storage')
get_ipython().system('gcloud config set project striking-ruler-461123-e3')
get_ipython().system('gcloud auth application-default login')

# === SET DATE RANGE ===
start_date = datetime(2025, 7, 3)
end_date = datetime.today()
date = start_date

# === SETUP GCS CLIENT ===
client = storage.Client()
bucket = client.bucket(bucket_name)


# In[12]:


while date <= end_date:
    date_str = date.strftime("%Y-%m-%d")
    print(f"\nFetching data for {date_str}...")

    try:
        df = yf.download(
            tickers=ticker,
            interval="1m",
            start=date_str,
            end=(date + timedelta(days=1)).strftime("%Y-%m-%d"),
            progress=False,
            threads=True,
        )

        if df.empty:
            print(f"⛔ No data available for {date_str} (weekend or holiday)")
        else:
            df.reset_index(inplace=True)
            local_file = f"{ticker}_1min_{date_str}.csv"
            df.to_csv(local_file, index=False)

            # Upload to GCS
            blob = bucket.blob(f"{gcs_prefix}{date_str}.csv")
            blob.upload_from_filename(local_file)
            print(f"✅ Uploaded to gs://{bucket_name}/{gcs_prefix}{date_str}.csv")
        
        # Sleep to avoid rate-limiting
        time.sleep(2)

    except Exception as e:
        print(f"⚠️ Error for {date_str}: {e}")

    # Move to next day
    date += timedelta(days=1)

