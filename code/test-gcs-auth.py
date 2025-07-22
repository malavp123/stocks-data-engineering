from google.cloud import storage
from dotenv import load_dotenv
import os

load_dotenv()

def list_buckets():
    client = storage.Client()
    buckets = list(client.list_buckets())
    print("✅ Auth Success! Found buckets:")
    for bucket in buckets:
        print(f" - {bucket.name}")

if __name__ == "__main__":
    list_buckets()
