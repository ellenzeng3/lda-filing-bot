import requests
from datetime import date
from dotenv import load_dotenv
import os 
from pathlib import Path
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo 
import holidays

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

def fetch_quarter(): 
    """
    Returns one of: 'first_quarter', 'second_quarter', 'third_quarter', 'fourth_quarter'
    using these windows (lower bound exclusive, upper bound inclusive):

      Jan 21 < d <= Apr 21  -> first_quarter
      Apr 21 < d <= Jul 21  -> second_quarter
      Jul 21 < d <= Oct 20  -> third_quarter
      Oct 20 < d <= Jan 20  -> fourth_quarter

    """
    today = datetime.now(ZoneInfo("America/New_York")).date()
    print(today)

    # Define canonical quarter boundary dates
    boundaries = {
        "first_quarter": date(THIS_YEAR, 4, 21),
        "second_quarter": date(THIS_YEAR, 7, 21),
        "third_quarter": date(THIS_YEAR, 10, 20),
        "fourth_quarter": date(THIS_YEAR + 1, 1, 20),
    }

    # US federal holidays for THIS_YEAR and next year (for Jan 20 boundary)
    us_holidays = holidays.UnitedStates(years=[THIS_YEAR, THIS_YEAR + 1])

    def shift_to_next_business_day(d: date) -> date:
        """If a date falls on a weekend or US federal holiday, move to next weekday that's not a holiday."""
        while d.weekday() >= 5 or d in us_holidays:
            d = d + timedelta(days=1)
        return d

    b1 = shift_to_next_business_day(date(THIS_YEAR, 4, 21))
    b2 = shift_to_next_business_day(date(THIS_YEAR, 7, 21))
    b3 = shift_to_next_business_day(date(THIS_YEAR, 10, 20))
    b4 = shift_to_next_business_day(date(THIS_YEAR + 1, 1, 20))

    if date(THIS_YEAR, 1, 21) < today <= b1:
        return "first_quarter"
    elif b1 < today <= b2:
        return "second_quarter"
    elif b2 < today <= b3:
        return "third_quarter"
    else:
        return "fourth_quarter"



def fetch_all_filings(seen_ids=None):
    # print(f"[fetch] year={year}", flush=True)
    url = BASE_URL
    page = 1
    all_results = []
    filing_period = fetch_quarter()

    while True:
        try:
            params = {"filing_year": THIS_YEAR, "page": page, "filing_period": filing_period, "ordering":"-dt_posted"}
            r = session.get(url, params=params, timeout=20)
            print(f"[fetch] GET {r.url} -> {r.status_code}", flush=True)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"Error: {e}")
            break 

        results = data.get("results") or []
        # stop fetching when we encounter a filing we've already seen.
        if seen_ids:
            for res in results:
                fid = res.get("filing_uuid")
                if fid and fid in seen_ids:
                    print(f"Encountered seen filing {fid}; stopping fetch.")
                    return all_results
                all_results.append(res)
        else:
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