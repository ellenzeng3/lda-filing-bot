import sqlite3 
from extract import get_uuid, get_filing_year, get_filing_document_url, get_filing_period, get_registrant_name, get_client_name, get_income, get_expenses, get_lobbying_descriptions, get_lobbyist_names
from fetch import fetch_all_filings
import os as _os
import re
from difflib import SequenceMatcher
from pathlib import Path
DB_PATH = _os.getenv("DATABASE_PATH", "filings.db")


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
                relevant TEXT
            )
        """) 

    print("Database created/updated.")
    conn.commit()
    conn.close()

# Update the database with new LDA filings
def update_db(): 

    conn = sqlite3.connect(DB_PATH, check_same_thread=False) 
    c = conn.cursor()
    
    try:
        c.execute("SELECT filing_uuid FROM filings")
        seen_ids = {row[0] for row in c.fetchall()} 
    except Exception as e:
        seen_ids = set()
    print(f"Seen IDs loaded: {len(seen_ids)}")

    try:
        # Pass seen_ids so fetch_all_filings can stop paging once it reaches
        # filings we've already processed 
        call = fetch_all_filings(seen_ids=seen_ids)
    except Exception as e:
        print("Error fetching filings:", e)
        return

    print("Fetched filings:", len(call))

    # Process each filing, checking if it's already in the database    
    posts = [] 
    num_new = 0
    for filing in call: 
        uuid = get_uuid(filing)
        if uuid in seen_ids:
            break # assuming filings are in reverse chronological order
        else: 
            num_new += 1
            seen_ids.add(uuid)
            try: 
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
            }
                # Determine relevance 
                relevance = is_relevant(filing_data)
                filing_data['relevant'] = 'yes' if relevance else 'no'
                print("Saving client:", filing_data["client_name"])
                save_filing_to_db(conn, filing_data) 

                if filing_data["income"] is None and filing_data["expenses"] is None:
                    pass 
                else:
                    print("Posting client:", filing_data["client_name"])
                    posts.append(filing_data)  # add to post_slack

            except Exception as e:
                print(f"Error processing filing {uuid}: {e}")
                continue
    print(f"New filings processed: {num_new}")
    conn.close() 
    
    # Import post_slack lazily to avoid circular imports with post.py
    try:
        from post import post_slack, post_done
    except Exception:
        post_slack = None

    if posts:
        for filing_data in posts:
            if post_slack:
                post_slack(filing_data) 
    else:
        print("No new relevant filings to post.")


def is_relevant(filing_data: dict) -> bool: 
    description = (filing_data.get("lobbying_descriptions") or "").lower()
    client = (filing_data.get("client_name") or "").lower()
    registrant = (filing_data.get("registrant_name") or "").lower()
 
    tech_terms = (
        "tech", "technology", "technologies", "privacy", "data", "data protection",
        "cybersecurity", "social media", "internet", "ai",  
        "artificial intelligence", "platform", "gdpr"
    )

    big_tech = (
        "amazon", "google", "meta", "facebook", "apple", "microsoft", "twitter",
        "tesla", "netflix", "ibm", "oracle", "intel", "nvidia", "databricks"
    )

    def normalize_name(s: str) -> str:
        s = (s or "").lower()
        # Remove common 'doing business as' patterns
        s = re.sub(r"\b(d/b/a|dba|doing business as)\b", "", s)
        # Remove punctuation
        s = re.sub(r"[^a-z0-9\s]", " ", s)
        # Remove common corporate suffixes
        s = re.sub(r"\b(inc|incorporated|llc|l\.l\.c|ltd|corp|corporation|co|llp|plc|gmbh|sa)\b", "", s)
        # Collapse whitespace
        s = re.sub(r"\s+", " ", s).strip()
        # print(s)
        return s

    norm_client = normalize_name(client)
    norm_registrant = normalize_name(registrant) 

    if (
        any(term in description for term in tech_terms)
        or any(term in norm_client for term in big_tech)
        or any(term in norm_registrant for term in big_tech)
    ):
        print("Posting filing for client:", filing_data.get("client_name"))
        return True
    print("Skipping filing for client:", filing_data.get("client_name"))

    return False
 
def save_filing_to_db(conn, filing_data: dict):
    """Insert or replace a filing record in the database."""
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO filings
        (filing_uuid, filing_document_url, filing_year, filing_period, registrant_name, client_name, 
            income, expenses, lobbying_descriptions, lobbyist_names, relevant)
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
        filing_data["relevant"],
    ))
    conn.commit()

