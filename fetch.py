import requests
from datetime import date
from dotenv import load_dotenv
import os 
from pathlib import Path
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from datetime import date, datetime
from zoneinfo import ZoneInfo 

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
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 502, 503, 504],
    allowed_methods=["GET"],
)

session.mount("https://", HTTPAdapter(max_retries=retry))
BASE_URL = "https://lda.senate.gov/api/v1/filings/"
THIS_YEAR = date.today().year

# ─── Main fetches ───────────────────────────────────────────────────────────────

# def fetch_year():
#     today = date.today()
#     return today.year

def fetch_quarter(): 
    """
    Returns one of: 'first_quarter', 'second_quarter', 'third_quarter', 'fourth_quarter'
    using these windows (lower bound exclusive, upper bound inclusive):

      Jan 21, 2025 < d <= Apr 21, 2025  -> first_quarter
      Apr 21, 2025 < d <= Jul 21, 2025  -> second_quarter
      Jul 21, 2025 < d <= Oct 20, 2025  -> third_quarter
      Oct 20, 2025 < d <= Jan 20, 2026  -> fourth_quarter

    """
    today = datetime.now(ZoneInfo("America/New_York")).date()
    print(today)

    if date(THIS_YEAR, 1, 21) < today <= date(THIS_YEAR, 4, 21):
        return "first_quarter"
    elif date(THIS_YEAR, 4, 21) < today <= date(THIS_YEAR, 7, 21):
        return "second_quarter"
    elif date(THIS_YEAR, 7, 21) < today <= date(THIS_YEAR, 10, 20):
        return "third_quarter"
    else:
        return "fourth_quarter"




def fetch_all_filings():
    # print(f"[fetch] year={year}", flush=True)
    url = BASE_URL
    page = 1
    all_results = []
    filing_period = fetch_quarter()

    while True:
        try:
            params = {"filing_year": THIS_YEAR, "page": page, "filing_period": filing_period}
            r = session.get(url, params=params, timeout=20)
            print(f"[fetch] GET {r.url} -> {r.status_code}", flush=True)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"Error: {e}")
            break 

        results = data.get("results") or []
        all_results.extend(results) 
        page += 1

    return all_results
 

 
    
def fetch_filing(uuid): 
    try:
        url = f"https://lda.senate.gov/api/v1/filings/{uuid}/"
        
        params = { 
            "filing_uuid": uuid 
        } 
        
        r = session.get(url, params=params, timeout=10)
        # r = session.get(url, timeout=10)

        r.raise_for_status() 
        return r.json() 

    except Exception as e:
        print(f"Error fetching filings")
        print (e)
        return []