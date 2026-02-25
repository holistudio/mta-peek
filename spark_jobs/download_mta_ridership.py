import os
import time
import requests
import pandas as pd
from pathlib import Path

APP_TOKEN = os.environ["SOCRATA_APP_TOKEN"]
DATA_RAW  = Path(os.environ["DATA_RAW"])

ENDPOINT  = "https://data.ny.gov/resource/5wq4-mkjj.csv"
PAGE_SIZE = 50_000
YEAR      = "2025"
WHERE     = f"transit_timestamp >= '{YEAR}-01-01T00:00:00' AND transit_timestamp < '{int(YEAR)+1}-01-01T00:00:00'"
OUT_FILE  = DATA_RAW / f"ridership_{YEAR}.parquet"

def fetch_page(offset: int, session: requests.Session) -> pd.DataFrame:
    params = {
        "$where":  WHERE,
        "$limit":  PAGE_SIZE,
        "$offset": offset,
        "$order":  "transit_timestamp ASC",
    }
    headers = {"X-App-Token": APP_TOKEN}
    
    for attempt in range(3):
        try:
            resp = session.get(ENDPOINT, params=params, headers=headers, timeout=120)
            resp.raise_for_status()
            from io import StringIO
            return pd.read_csv(StringIO(resp.text))
        except Exception as e:
            if attempt == 2:
                raise
            wait = 2 ** attempt  # 1s, then 2s
            print(f"  Attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)

def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    
    all_chunks = []
    offset = 0
    
    while True:
        print(f"Fetching rows {offset:,} – {offset + PAGE_SIZE:,}...")
        chunk = fetch_page(offset, session)
        
        if chunk.empty:
            print("Empty page returned — download complete.")
            break
        
        all_chunks.append(chunk)
        print(f"  Got {len(chunk):,} rows. Total so far: {sum(len(c) for c in all_chunks):,}")
        
        if len(chunk) < PAGE_SIZE:
            print("Partial page — this is the last page.")
            break
        
        offset += PAGE_SIZE
    
    print("Concatenating and writing to Parquet...")
    df = pd.concat(all_chunks, ignore_index=True)
    df.to_parquet(OUT_FILE, index=False)
    print(f"Done. {len(df):,} rows written to {OUT_FILE}")

if __name__ == "__main__":
    main()