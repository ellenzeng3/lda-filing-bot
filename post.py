from slack_sdk import WebClient 
import os
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask 
from slackeventsapi import SlackEventAdapter
from datetime import date
from decimal import Decimal, InvalidOperation


# Set up Slack app
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)
app = Flask(__name__)
slack_event_adapter = SlackEventAdapter(os.environ['SIGNING_SECRET'],'/slack/events',app) 
client = WebClient(token=os.environ['SLACK_TOKEN']) 

SLACK_CHANNEL = "#lda-filings"
TODAY = date.today()
FORMATTED_TODAY = f"{TODAY.strftime('%B')} {TODAY.day}, {TODAY.year}"

def format_dollars(val):
    try:
        d = Decimal(str(val))
    except (InvalidOperation, ValueError):
        return None
    q = d.quantize(Decimal("1")) if d == d.quantize(Decimal("1")) else d.quantize(Decimal("0.01"))
    return f"${q:,.0f}" if isinstance(q, Decimal) and q == q.quantize(Decimal("1")) else f"${q:,.2f}"


def post_slack(filing_data):
    if not filing_data:
        client.chat_postMessage(
            channel = SLACK_CHANNEL,
            text = f"No new tech-related filings found on {FORMATTED_TODAY}"
        )
    
    try:  
        reg = (filing_data.get("registrant_name") or "").strip()
        client = filing_data.get("client_name") 
        url = filing_data.get("filing_document_url") or ""
        lobbyists = filing_data.get("lobbyist_names") or []

        income_str = format_dollars(filing_data.get("income"))
        expenses_str = format_dollars(f.get("expenses"))

        # build the amount phrase
        if income_str and expenses_str:
            amt_phrase = f"disclosed *{income_str}* in income and *{expenses_str}* in expenses"
        elif income_str:
            amt_phrase = f"disclosed *{income_str}* in income"
        else:
            amt_phrase = f"disclosed *{expenses_str}* in expenses" 

        filing_phrase = f"<{url}|LDA filing>" if url else "LDA filing"
 
        header = f"*{reg}* {amt_phrase} in the {filing_phrase} for *{client}*."

        # Lobbyists list 
        lobby_block = ""
        if lobbyists:
            bullets = "\n".join(f"• {name}" for name in lobbyists if name)
            lobby_block = f"\n\n*Lobbyists:*\n{bullets}"

        client.chat_postMessage(
            channel = SLACK_CHANNEL,
            text = f"{header}{lobby_block}"
        )
        return True
    except Exception as e:
        print(f"Error posting filing: {e}")