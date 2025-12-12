import sqlite3
import os
import re
import io
import csv
import requests
from multiprocessing import Process
from pathlib import Path
from dotenv import load_dotenv
from re import sub
from flask import make_response, request
from flask import Flask as _Flask
from slack_sdk import WebClient
from slackeventsapi import SlackEventAdapter 
from db_actions import update_db, initialize_db 
from extract import parse_command, curr_quarter, curr_year

# ─── Main ──────────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DATABASE_PATH", "/app/data/filings.db")
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)
app = _Flask(__name__)
slack_event_adapter = SlackEventAdapter(os.environ.get('SIGNING_SECRET', ''), '/slack/events', app)
client = WebClient(token=os.environ.get('SLACK_TOKEN'))

# Ensure DB is initialized when the app module is imported by Gunicorn/Flask CLI
try:
    initialize_db()
except Exception as e:
    print(f"Warning: initialize_db() raised an exception at import time: {e}")

@slack_event_adapter.on('app_mention')
def handle_mention(payload):
    print("Mention received")
    event = payload.get('event', {}) 
    user = event.get('user')
    channel = event.get('channel') 
    text = event.get('text', '')
    cleaned = sub(r'<@[^>]+>', '', text).strip().lower()

    # return if no 'post' command
    if 'post' not in cleaned:
        client.chat_postEphemeral(
            channel=channel,
            user=user,
            text="To post filings, mention me in a message like so: '@LobbyingBot post [quarter] [year]'. " \
            "For example, '@LobbyingBot post q1 2024' will post filings from quarter 1 of 2024. If no quarter/year is specified, filings will be pulled from the current quarter."
        )
        return make_response("", 200)
    period, year = parse_command(cleaned)

    # run compile in background process so we return quickly to Slack
    p = Process(target=compile_filings, args=(payload, period, year), daemon=True)
    p.start()
    
    return make_response("", 200)

def compile_filings(payload, filing_period=None, year=None):
    event = payload.get('event', {}) 
    user = event.get('user')
    channel = event.get('channel') 
    
    # Use provided filing_period/year if given, otherwise default to current
    if not filing_period:
        filing_period = curr_quarter()
    if not year:
        year = curr_year()
    
    # Ensure DB is updated before posting
    initialize_db()
    update_db(filing_period=filing_period, year=year)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """SELECT client_name, registrant_name, income, expenses, dt_posted, 
            lobbyist_names, filing_document_url, lobbying_descriptions, 
            filing_period, filing_year
            FROM filings
            WHERE filing_period = ? AND filing_year = ?
        """,
        (filing_period, year),
    )
    rows = c.fetchall()
    if not rows:
        try:
            client.chat_postMessage(
                channel=channel,
                user=user,
                text=f"No filings found for {filing_period} {year}."
            )
        except Exception as e:
            print(f"Error sending no-results message: {e}")
        return


    try: 
        client.chat_postMessage(
            channel=channel,
            user=user,
            text=f"Posting tech filings from {filing_period} {year}."
        )
    except Exception as e:
        print(f"Error sending starting-post message: {e}")

    # Build CSV 
    sio = io.StringIO()
    writer = csv.writer(sio)

    header = [
        "client_name", "registrant_name", "income", "expenses", "dt_posted", 
        "lobbyist_names", "filing_document_url", "lobbying_descriptions",
        "filing_period", "filing_year"
    ]
    writer.writerow(header)  
    for row in rows:
        cleaned_row = ["" if v is None else v for v in row]
        writer.writerow(cleaned_row)
    csv_content = sio.getvalue()
    sio.close()

    filename = f"lda_filings_{filing_period}_{year}.csv" 

    # Get the external upload URL
    try:
        upload_url_response = client.files_getUploadURLExternal(
            token= os.environ.get('SLACK_TOKEN'), 
            filename=filename,
            length=len(csv_content)
        )
        upload_url = upload_url_response['upload_url']
        file_id = upload_url_response['file_id'] 
    except Exception as e:
        print(f"Error getting external upload URL: {e}")
        return

    try:
        file_bytes = csv_content.encode('utf-8')
        response = requests.post(
            upload_url, files={"file": (filename, io.BytesIO(file_bytes))}
            ) 
    except Exception as e:
        print(f"Error uploading file to URL: {e}")
        return None

    try:
        client.files_completeUploadExternal(
            token= os.environ.get('SLACK_TOKEN'), 
            files=[{"id":file_id, "title":filename}],
            channel_id= channel
        ) 
    except Exception as e:
        print(f"Error completing file upload: {e}")

    

if __name__ == '__main__': 
    app.run( port=8080, debug=True) 
     

    