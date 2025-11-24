import sqlite3
from slack_sdk import WebClient 
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask 
from json import loads
from slackeventsapi import SlackEventAdapter
import os 
from multiprocessing import Process
from fetch import fetch_quarter, THIS_YEAR
from slack_sdk import WebClient
from slackeventsapi import SlackEventAdapter
from dotenv import load_dotenv
from pathlib import Path as _Path
from flask import Flask as _Flask, make_response, request
import requests
import io, csv
from re import sub 
from datetime import date
from db_actions import update_db, initialize_db 

# ─── Main ──────────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DATABASE_PATH", "/app/data/filings.db")
env_path = _Path('.') / '.env'
load_dotenv(dotenv_path=env_path)
app = _Flask(__name__)
slack_event_adapter = SlackEventAdapter(os.environ.get('SIGNING_SECRET', ''), '/slack/events', app)
client = WebClient(token=os.environ.get('SLACK_TOKEN'))
CHANNEL = "#private-test-channel"

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
        return client.chat_postEphemeral(
            channel=channel,
            user=user,
            text="To post relevant filings, use '@LDAFilingBot post'. To post all filings, use '@LDAFilingBot post all'."
        ) 
    
    # 'post all': posting all filings from quarter
    if 'all' in cleaned:
        p = Process(target=compile_filings, args=(payload, 'all'), daemon=True)
    
    # 'post': posting only relevant filings from quarter
    else: 
        p = Process(target=compile_filings, args=(payload, 'post'), daemon=True)
        p.start()
    
    return make_response("", 200)

def compile_filings(payload, type):
    event = payload.get('event', {}) 
    user = event.get('user')
    channel = event.get('channel') 
    filing_period = fetch_quarter()
    year = THIS_YEAR
    
    # Ensure DB is updated before posting
    update_db()

    # if 'all' command, post all data
    if type == 'all': 
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            """SELECT registrant_name, client_name, filing_document_url,  lobbying_descriptions, 
                    lobbyist_names, income, expenses, filing_period, filing_year, relevant
               FROM filings
               WHERE filing_period = ? AND filing_year = ? 
            """,
            (filing_period, year),
        )
        rows = c.fetchall()

        client.chat_postEphemeral(
            channel=channel,
            user=user,
            text="Posting all filings from this quarter. For only relevant filings use '@LDAFilingBot post'."
        )

    # if no 'all' command, post only relevant data
    else:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            """SELECT registrant_name, client_name, filing_document_url,  lobbying_descriptions, 
                    lobbyist_names, income, expenses, filing_period, filing_year, relevant
               FROM filings
               WHERE filing_period = ? AND filing_year = ?  AND relevant = 'yes'
            """,
            (filing_period, year),
        )
        rows = c.fetchall()
        client.chat_postEphemeral(
            channel=channel,
            user=user,
            text="Posting relevant filings from this quarter. For all filings use '@LDAFilingBot post all'."
        )

    conn.close()   

    if not rows:
        try:
            client.chat_postEphemeral(
                channel=channel,
                user=user,
                text=f"No filings found for {filing_period} {year}."
            )
        except Exception as e:
            print(f"Error sending ephemeral no-results message: {e}")
        return

    # Build CSV content
    sio = io.StringIO()
    writer = csv.writer(sio)

    # registrant_name, client_name, filing_document_url,  lobbying_descriptions, 
                #    lobbyist_names, income, expenses, filing_period, filing_year, relevant
    header = [
        "registrant_name",
        "client_name",
        "filing_document_url", 
        "lobbying_descriptions",
        "lobbyist_names",
        "income",
        "expenses", 
        "filing_period",
        "filing_year", 
        "relevant"
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

    # Upload the file content to the external URL
        response = requests.post(upload_url, files={"file": (filename, io.BytesIO(file_bytes))}) 
        
    except Exception as e:
        print(f"Error uploading file to URL: {e}")
        return None

    try:
        complete_response = client.files_completeUploadExternal(
            token= os.environ.get('SLACK_TOKEN'), 
            files=[{"id":file_id, "title":filename}],
            channel_id= channel
        ) 
    except Exception as e:
        print(f"Error completing file upload: {e}")

    

if __name__ == '__main__': 
    app.run( port=8080, debug=True) 
     

    