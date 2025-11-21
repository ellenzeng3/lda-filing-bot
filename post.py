from slack_sdk import WebClient 
import os
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask 
from slackeventsapi import SlackEventAdapter
from datetime import datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

# Set up Slack app
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)
app = Flask(__name__)
slack_event_adapter = SlackEventAdapter(os.environ['SIGNING_SECRET'],'/slack/events',app) 
client = WebClient(token=os.environ['SLACK_TOKEN']) 

SLACK_CHANNEL = "#lda-filings"
TODAY = datetime.now(ZoneInfo("America/New_York"))
FORMATTED_TODAY = f"{TODAY.strftime('%B')} {TODAY.day}, {TODAY.year}"

def format_dollars(val):
    try:
        d = Decimal(str(val))
    except (InvalidOperation, ValueError):
        return None
    q = d.quantize(Decimal("1")) if d == d.quantize(Decimal("1")) else d.quantize(Decimal("0.01"))
    return f"${q:,.0f}" if isinstance(q, Decimal) and q == q.quantize(Decimal("1")) else f"${q:,.2f}"


def post_slack(filing_data):
    if filing_data is None:
        try: 
            client.chat_postMessage(
            channel = SLACK_CHANNEL,
            text = f"No new tech-related filings found on {FORMATTED_TODAY}"
            )
        except Exception as e:
            print(f"Error posting no-filings message: {e}")
        return True
    
    try:  
        reg = (filing_data.get("registrant_name") or "").strip()
        client_name = filing_data.get("client_name") 
        url = filing_data.get("filing_document_url") or ""
        lobbyists_str = filing_data.get("lobbyist_names") or []
        lobbyists = [n.strip() for n in lobbyists_str.split(",") if n.strip()]
        lobbyist_lines = "\n".join([f"• {l}" for l in lobbyists]) if lobbyists else "• —"


        income_str = format_dollars(filing_data.get("income"))
        expenses_str = format_dollars(filing_data.get("expenses"))

        # build the amount phrase
        if income_str and expenses_str:
            amt_phrase = f"disclosed {income_str} in income and {expenses_str} in expenses"
        elif income_str:
            amt_phrase = f"disclosed {income_str} in income"
        else:
            amt_phrase = f"disclosed {expenses_str} in expenses" 

        filing_phrase = f"<{url}|LDA filing>" if url else "LDA filing"
 
        header = f"*{reg}* {amt_phrase} in the {filing_phrase} for {client_name}, and the following lobbyists:"

        # Lobbyists list 
        lobby_block = ""
        if lobbyists:
            bullets = "\n".join(f"• {name}" for name in lobbyists if name)
            lobby_block = f"\n{lobbyist_lines}"

        client.chat_postMessage(
            channel = SLACK_CHANNEL,
            text = f"{header}{lobby_block}"
        )
        return True
    except Exception as e:
        print(f"Error posting filing: {e}")


# --- CSV posting -----------------------------------------------------------
def post_csv(requesting_channel=None, requesting_user=None):
    """Fetch latest filings, compile CSV for current filing period, and upload to Slack.

    If `requesting_user` is provided, send an ephemeral confirmation.
    """
    import sqlite3
    import io
    import csv
    try:
        import lda_bot
        from fetch import fetch_quarter, THIS_YEAR
    except Exception as e:
        print(f"Error importing modules for post_csv: {e}")
        return False

    # Run update to ensure DB is current
    try:
        lda_bot.update_db()
    except Exception as e:
        print(f"Error running update_db: {e}")

    filing_period = fetch_quarter()
    year = THIS_YEAR

    # Connect to DB and retrieve filings for current period/year
    try:
        conn = sqlite3.connect(lda_bot.DB_PATH)
        c = conn.cursor()
        c.execute(
            """SELECT filing_uuid, filing_document_url, filing_year, filing_period,
                      registrant_name, client_name, income, expenses, lobbying_descriptions, lobbyist_names
               FROM filings
               WHERE filing_period = ? AND filing_year = ?
            """,
            (filing_period, year),
        )
        rows = c.fetchall()
    except Exception as e:
        print(f"Error querying DB for CSV: {e}")
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not rows:
        # Inform the requesting user there's nothing to post
        if requesting_channel and requesting_user:
            try:
                client.chat_postEphemeral(
                    channel=requesting_channel,
                    user=requesting_user,
                    text=f"No filings found for {filing_period} {year}."
                )
            except Exception as e:
                print(f"Error sending ephemeral no-results message: {e}")
        return False

    # Build CSV in memory
    sio = io.StringIO()
    writer = csv.writer(sio)
    header = [
        "filing_uuid",
        "filing_document_url",
        "filing_year",
        "filing_period",
        "registrant_name",
        "client_name",
        "income",
        "expenses",
        "lobbying_descriptions",
        "lobbyist_names",
    ]
    writer.writerow(header)
    for row in rows:
        # Normalize None to empty strings
        cleaned = ["" if v is None else v for v in row]
        writer.writerow(cleaned)

    csv_content = sio.getvalue()
    sio.close()

    filename = f"lda_filings_{filing_period}_{year}.csv"

    try:
        client.files_upload(
            channels=SLACK_CHANNEL,
            content=csv_content,
            filename=filename,
            title=f"LDA filings — {filing_period} {year}",
            initial_comment=f"LDA filings for {filing_period} {year} — uploaded by bot",
        )
        # Tell requester we're done
        if requesting_channel and requesting_user:
            try:
                client.chat_postEphemeral(
                    channel=requesting_channel,
                    user=requesting_user,
                    text=f"Uploaded CSV for {filing_period} {year} to {SLACK_CHANNEL}"
                )
            except Exception as e:
                print(f"Error sending ephemeral confirmation: {e}")
        return True
    except Exception as e:
        print(f"Error uploading CSV to Slack: {e}")
        return False