import sqlite3 
from extract import get_uuid, get_filing_year, get_filing_document_url, get_filing_period, get_registrant_name, get_client_name, get_income, get_expenses, get_lobbying_descriptions, get_lobbyist_names, get_date_posted
from fetch import fetch_all_filings
import os as _os
import re
# difflib.SequenceMatcher removed - not used
from pathlib import Path
DB_PATH = _os.getenv("DATABASE_PATH", "filings.db")

def is_exact_company(client_name, company):
    # For single-letter companies, require exact match
    if len(company) <= 2:
        return client_name.strip().lower() == company.lower()
    # Otherwise, use word-boundary regex
    pattern = r"\b" + re.escape(company.lower()) + r"\b"
    return re.search(pattern, client_name.lower()) is not None 

def initialize_db():
    if not Path("/app/data").is_dir():
        raise RuntimeError(
            "/app/data volume is not mounted – aborting."
        )
        
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)

    # delete existing table 
    # conn.execute("DROP TABLE IF EXISTS filings")

    with conn: 
        conn.execute("""
            CREATE TABLE IF NOT EXISTS filings (
                filing_uuid TEXT PRIMARY KEY,
                filing_document_url TEXT,
                filing_year INTEGER,
                filing_period TEXT,
                registrant_name TEXT,
                client_name TEXT,
                income REAL,
                expenses REAL,
                lobbying_descriptions TEXT,
                lobbyist_names TEXT,
                dt_posted TEXT
            )
        """)

    print("Database created/updated.")
    conn.commit()
    conn.close()

# Update the database with new LDA filings
def update_db(filing_period, year): 
    """
    Fetch all filings for tech companies for the given quarter/year, deduplicate, and save new filings to the database.
    """

    tech_companies = (
        "amazon", "alphabet", "apple", "google", "alphabet", "google cloud",
        "waymo", "wing aviation", "verily life sciences", "deepmind",
        "bytedance", "tiktok", "x", "twitter", "discord", "microsoft",
        "linkedin", "technet", "netchoice", "snap", 
        "openai", "internet works", "meta", "facebook", "tesla", "nvidia",
    )
    conn = sqlite3.connect(DB_PATH, check_same_thread=False) 
    c = conn.cursor()
    
    try:
        c.execute("SELECT filing_uuid FROM filings")
        seen_ids = {row[0] for row in c.fetchall()} 
    except Exception as e:
        seen_ids = set()
    print(f"Seen IDs loaded: {len(seen_ids)}")

    all_filings = []
    for company in tech_companies: 
        try:
            call = fetch_all_filings(seen_ids=seen_ids, filing_period=filing_period, 
                                     year=year, client_name=company)
            all_filings.extend(call)

            for filing in call:
                if not is_exact_company(get_client_name(filing), company):
                    continue
                uuid = get_uuid(filing)
                if uuid in seen_ids:
                    continue
                seen_ids.add(uuid)
                filing_data =  {
                    "filing_uuid": get_uuid(filing),
                    "filing_year": get_filing_year(filing),
                    "filing_period": get_filing_period(filing),
                    "registrant_name": get_registrant_name(filing),
                    "client_name": get_client_name(filing),
                    "lobbying_descriptions": get_lobbying_descriptions(filing),
                    "income": get_income(filing),
                    "expenses": get_expenses(filing),
                    "filing_document_url": get_filing_document_url(filing),
                    "lobbyist_names": get_lobbyist_names(filing),
                    "dt_posted": get_date_posted(filing).split("T")[0]
                }
                save_filing_to_db(conn, filing_data)

        except Exception as e:
            print(f"Error fetching filings for registrant_name={company}: {e}") 

    print("Fetched filings:", len(all_filings))
 
    conn.close()

 
def save_filing_to_db(conn, filing_data: dict):
    """
    Insert or replace a filing record in the database.
    """
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO filings
        (filing_uuid, filing_document_url, filing_year, filing_period, registrant_name, client_name,
            income, expenses, lobbying_descriptions, lobbyist_names, dt_posted)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        filing_data["filing_uuid"],
        filing_data["filing_document_url"],
        filing_data["filing_year"],
        filing_data["filing_period"],
        filing_data["registrant_name"],
        filing_data["client_name"],
        filing_data["income"],
        filing_data["expenses"],
        filing_data["lobbying_descriptions"],
        filing_data["lobbyist_names"],
        filing_data["dt_posted"]
    ))
    conn.commit()

