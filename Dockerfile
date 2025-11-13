FROM python:3.12-slim AS builder

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential gcc g++ python3-dev git && \ 
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Add application code
COPY . .

# Folder that will be backed by the Fly volume
# RUN mkdir -p /data

CMD ["python", "lda_bot.py", "update"]
