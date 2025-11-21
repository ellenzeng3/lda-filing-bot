import time
from lda_bot import update_db 

SECONDS_PER_DAY = 24 * 60 * 60

while True:
    try:
        update_db()  # your "check LDA filings and update DB + Slack" logic
    except Exception as e:
        print(f"Daily update failed: {e}", flush=True)

    time.sleep(SECONDS_PER_DAY)