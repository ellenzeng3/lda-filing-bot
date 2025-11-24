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

SLACK_CHANNEL = "#private-test-channel"
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
 
        header = f"*{reg}* {amt_phrase} in the {filing_phrase} for {client_name}" 
        client.chat_postMessage(
            channel = SLACK_CHANNEL,
            text = f"{header}"
        )
        return True
    except Exception as e:
        print(f"Error posting filing: {e}")


def post_done():
    try:
        client.chat_postMessage(
            channel=SLACK_CHANNEL,
            text=f"Finished posting LDA filings for {FORMATTED_TODAY}."
        )
        return True
    except Exception as e:
        print(f"Error posting done message: {e}")
        return False
    