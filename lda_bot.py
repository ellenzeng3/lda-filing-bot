import sqlite3
from slack_sdk import WebClient 
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask 
from slackeventsapi import SlackEventAdapter
import os 
from fetch import fetch_all_filings, fetch_filing, fetch_quarter, THIS_YEAR
from slack_sdk import WebClient
from slackeventsapi import SlackEventAdapter
from dotenv import load_dotenv
from pathlib import Path as _Path
from flask import request, jsonify, Flask as _Flask
import requests
import io, csv
from re import sub 
from datetime import date
from db_actions import update_db, initialize_db 

# ─── Main ──────────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DATABASE_PATH", "/app/data/filings.db")

# Slack app setup (used for app_mention handler)
env_path = _Path('.') / '.env'
load_dotenv(dotenv_path=env_path)
app = _Flask(__name__)
slack_event_adapter = SlackEventAdapter(os.environ.get('SIGNING_SECRET', ''), '/slack/events', app)
client = WebClient(token=os.environ.get('SLACK_TOKEN'))
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL', '#lda-filings')


def scheduler():
    import time

    SECONDS_PER_DAY = 24 * 60 * 60
    print("Starting daily scheduler...")
    while True:
        try:
            update_db()  # your "check LDA filings and update DB + Slack" logic
        except Exception as e:
            print(f"Daily update failed: {e}", flush=True)

        time.sleep(SECONDS_PER_DAY)

@slack_event_adapter.on('app_mention')
def handle_mention(payload):
    print("App mention received.") 
    event = payload.get('event', {})
    # print(event)
    user = event.get('user')
    channel = event.get('channel') 
    text = event.get('text', '')
    cleaned = sub(r'<@[^>]+>', '', text).strip().lower()

    if 'post' not in cleaned:
        return

    # Acknowledge
    try:
        client.chat_postEphemeral(
            channel=channel,
            user=user,
            text="Working on compiling the CSV — I'll post it to the channel when ready."
        )
    except Exception as e:
        print(f"Error sending ephemeral ack: {e}")

    filing_period = fetch_quarter()
    year = THIS_YEAR

    # Query DB for current period
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            """SELECT registrant_name, client_name, filing_document_url,  lobbying_descriptions, 
                    lobbyist_names, income, expenses, filing_period, filing_year
               FROM filings
               WHERE filing_period = ? AND filing_year = ?  
            """,
            (filing_period, year),
        )
        rows = c.fetchall()
    except Exception as e:
        print(f"Error querying DB for CSV in handler: {e}")
        rows = []
    finally:
        try:
            conn.close()
        except Exception:
            pass

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
    header = [
        "registrant_name",
        "client_name",
        "filing_document_url", 
        "income",
        "expenses",
        "lobbying_descriptions",
        "lobbyist_names",
        "filing_period",
        "filing_year", 
    ]
    writer.writerow(header)
    for row in rows:
        cleaned_row = ["" if v is None else v for v in row]
        writer.writerow(cleaned_row)
    csv_content = sio.getvalue()
    sio.close()

    filename = f"lda_filings_{filing_period}_{year}.csv"

    # Step 1: Get the external upload URL
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

        # Step 2: Upload the file content to the external URL
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
        try:
            client.chat_postEphemeral(
                channel=channel,
                user=user,
                text=f"Uploaded CSV for {filing_period} {year} to {SLACK_CHANNEL}"
            )
        except Exception as e:
            print(f"Error sending ephemeral confirmation: {e}")
    except Exception as e:
        print(f"Error completing file upload: {e}")


@app.route('/slack/events', methods=['POST'])
def slack_events():
    if request.method == 'POST':
        data = request.json
        # Check for the challenge token
        if "challenge" in data:
            return jsonify({"challenge": data["challenge"]}) 
        slack_event_adapter.handle(request)
        return '', 200
    
    
if __name__ == '__main__':
    initialize_db()
    app.run(host='0.0.0.0', port=8080, debug=True)
    scheduler() 
     

    