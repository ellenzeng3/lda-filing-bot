import sqlite3
from slack_sdk import WebClient 
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask 
from slackeventsapi import SlackEventAdapter
import os
import sys    
import json
from fetch import fetch_all_filings, fetch_filing
from extract import get_uuid, get_filing_year, get_filing_document_url, get_filing_period, get_registrant_name, get_client_name, get_income, get_expenses, get_lobbying_descriptions, get_lobbyist_names
from datetime import date
from post import post_slack

# ─── Main ──────────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DATABASE_PATH", "/app/data/filings.db")


def main():
    if not Path("/app/data").is_dir():
        raise RuntimeError(
            "/app/data volume is not mounted – aborting."
        )
    
    # Create database if it doesn't exist
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
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
                lobbyist_names TEXT
            )
        """)

    if len(sys.argv) > 1 and sys.argv[1] == "update":
        print("Running update function")
        update_db()
    

    print("Database updated.")
    conn.commit()
    conn.close() 

# Update the database with new LDA filings
def update_db(): 
    

    conn = sqlite3.connect(DB_PATH, check_same_thread=False) 
    c = conn.cursor()

    # delete 10 rows for testing:
    c.execute("DELETE FROM filings WHERE filing_uuid IN (SELECT filing_uuid FROM filings LIMIT 100)")

    # Preload seen IDs or start fresh if table missing
    try:
        c.execute("SELECT filing_uuid FROM filings")
        seen_ids = {row[0] for row in c.fetchall()} 
    except Exception as e:
        seen_ids = set()
    print(f"Seen IDs loaded: {len(seen_ids)}")

    try:
        call = fetch_all_filings() # update  
    except Exception as e:
        print("Error fetching filings:", e)
        return

    print("Fetched filings:", len(call))

    # Process each filing, checking if it's already in the database    
    posts = [] 
    for filing in call: 
        uuid = get_uuid(filing)
        # print("Processing filing UUID:", uuid)
        if uuid in seen_ids:
            continue
        else: 
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
                save_filing_to_db(filing_data) 
                print(f"Saved filing {uuid} to database.")
                if should_post(filing_data) == True:
                    posts.append(filing_data)  # add to post_slack
                seen_ids.add(uuid)
            except Exception as e:
                print(f"Error processing filing {uuid}: {e}")
                continue

    if posts:
        for filing_data in posts:
            post_slack(filing_data)
    
    else:
        post_slack()
        
        
def should_post(filing_data: dict) -> bool:
    income = filing_data.get("income")
    expenses = filing_data.get("expenses")

    if income is None and expenses is None:
        return False
    
    description = filing_data.get("lobbying_descriptions")
    client = (filing_data.get("client_name")).lower()
    registrant = (filing_data.get("registrant_name")).lower()

    print(description, client, registrant)
    tech_terms = ("tech", "technology", "technologies", "privacy", "data", "cybersecurity", "social media", "internet", "ai", "artificial intelligence", "cloud computing", "software", "semiconductor", "e-commerce", "digital advertising", "smartphone")
    big_tech = ("amazon", "google", "meta", "facebook", "apple", "microsoft", "twitter", "tesla", "netflix", "ibm", "oracle", "intel", "nvidia")

    # Check if any keyword appears in the relevant field
    if (
        any(term in description for term in tech_terms)
        or any(term in client for term in big_tech)
        or any(term in registrant for term in big_tech)
    ):
        print("Posting filing for client:", filing_data.get("client_name"))
        return True
    print("Skipping filing for client:", filing_data.get("client_name"))
    return False

# save to database
def save_filing_to_db(filing_data: dict):
    """Insert or replace a filing record in the database."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False) 
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO filings
        (filing_uuid, filing_document_url, filing_year, filing_period, registrant_name, client_name, 
            income, expenses, lobbying_descriptions, lobbyist_names)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    ))
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main() 
     

    