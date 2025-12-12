import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo 
import holidays

def get_uuid(filing):
    return filing.get("filing_uuid")

def get_date_posted(filing):
    return filing.get("dt_posted")

def get_filing_date(filing):
    return filing.get("dt_posted")

def get_filing_year(filing):
    return filing.get("filing_year")

def get_filing_period(filing):
    return filing.get("filing_period")

def get_registrant_name(filing):
    return filing.get("registrant", {}).get("name")

def get_client_name(filing):
    return filing.get("client", {}).get("name")

def get_lobbying_descriptions(filing):
    return ", ".join(
        [a.get("description") for a in filing.get("lobbying_activities", []) if a.get("description")]
    )

def get_income(filing):
    return filing.get("income")

def get_expenses(filing):
    return filing.get("expenses")

def get_filing_document_url(filing):
    return filing.get("filing_document_url")

def get_lobbyist_names(filing):
    return ", ".join(
        sorted({
            f"{l.get('lobbyist', {}).get('first_name').title()} {l.get('lobbyist', {}).get('last_name').title()}"
            for a in filing.get("lobbying_activities", [])
            for l in a.get("lobbyists", [])
            if l.get("lobbyist", {}).get("first_name") and l.get("lobbyist", {}).get("last_name")
        })) 

def parse_command(s: str):
    """
    Parse a cleaned mention string and return the quarter and year
    """
    s = s.lower()
 
    # year
    m = re.search(r"\b(20\d{2})\b", s)
    year = int(m.group(1)) if m else None

    # quarter lookup tables
    quarter_words = {
        "1": 1, "1st": 1, "first": 1,
        "2": 2, "2nd": 2, "second": 2,
        "3": 3, "3rd": 3, "third": 3,
        "4": 4, "4th": 4, "fourth": 4,
    }
    quarter_map = {
        1: "first_quarter",
        2: "second_quarter",
        3: "third_quarter",
        4: "fourth_quarter",
    }

    # quarter
    q = None

    # q1, qtr1, quarter 1
    m = re.search(r"\bq(?:tr|uarter)?\s*(1|2|3|4)\b", s)
    if m:
        q = int(m.group(1))
    else:
        # first, 1st, second, etc.
        m = re.search(r"\b(1st|2nd|3rd|4th|first|second|third|fourth)\b", s)
        if m:
            q = quarter_words.get(m.group(1))

    period = quarter_map.get(q)

    return period, year

def curr_year():
    """Returns the current year as an integer."""
    return datetime.now(ZoneInfo("America/New_York")).year

def curr_quarter(): 
    """
    Returns one of: 'first_quarter', 'second_quarter', 'third_quarter', 'fourth_quarter'
    based on the current date.
    """
    today = datetime.now(ZoneInfo("America/New_York")).date()
    year_val = curr_year()  

    # US federal holidays for THIS_YEAR and next year (for Jan 20 boundary)
    us_holidays = holidays.UnitedStates(years=[year_val, year_val  + 1])
    
    def shift_to_next_business_day(d: date) -> date:
        """If a date falls on a weekend or US federal holiday, move to next weekday that's not a holiday."""
        while d.weekday() >= 5 or d in us_holidays:
            d = d + timedelta(days=1)
        return d

    b1 = shift_to_next_business_day(date(year_val, 4, 21))
    b2 = shift_to_next_business_day(date(year_val, 7, 21))
    b3 = shift_to_next_business_day(date(year_val, 10, 20))
    
    if date(year_val, 1, 21) < today <= b1:
        return "first_quarter"
    elif b1 < today <= b2:
        return "second_quarter"
    elif b2 < today <= b3:
        return "third_quarter"
    return "fourth_quarter"
