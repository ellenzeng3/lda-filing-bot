import requests
from datetime import date
from dotenv import load_dotenv
import os 
from pathlib import Path
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from extract import curr_quarter, curr_year 
# ─── Configuration ────────────────────────────────────────────────────────────

# Environment variables and session setup
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

API_KEY = os.getenv("LDA_API_KEY")
if not API_KEY:
    raise RuntimeError("LDA_API_KEY not set in environment variables.")

session = requests.Session()
session.headers.update({"X-Api-Key": API_KEY})
session.params = {"api_key": API_KEY}

retry = Retry(
    total=2,
    backoff_factor=0.5,
    status_forcelist=[429, 502, 503, 504],
    allowed_methods=["GET"],
)

session.mount("https://", HTTPAdapter(max_retries=retry))
BASE_URL = "https://lda.senate.gov/api/v1/filings/"

# ─── Main fetches ───────────────────────────────────────────────────────────────

def fetch_all_filings(seen_ids=None, filing_period=None, year=None, client_name=None):
    if not filing_period:
        filing_period = curr_quarter()
    if not year:
        year = curr_year()
     
    url = BASE_URL 
    all_results = []
    params = {
        "filing_year": year, 
        "filing_period": filing_period,
        "ordering": "-dt_posted",
    }  
    if client_name:
        params["client_name"] = client_name

    while url:
        try:
            # Use params only on the first request
            if params is not None:
                r = session.get(url, params=params, timeout=5)
                params = None
            else:
                r = session.get(url, timeout=5)

            if r.status_code == 404:
                print(f"No more pages (404 at {url}). Stopping pagination.")
                break

            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"Error: {e}")
            break 

        results = data.get("results") or [] 
        if not results:
            break

        if seen_ids is not None:  # stop on first seen b/c filings are ordered by dt_posted desc
            for result in results:
                fid = result.get("filing_uuid")
                if fid and fid in seen_ids:
                    print(f"Encountered seen filing {fid}; stopping fetch.")
                    return all_results
                all_results.append(result)
        else:
            all_results.extend(results)

        url = data.get("next")  # either will be full URL or None

    return all_results