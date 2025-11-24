FROM python:3.12-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential gcc g++ python3-dev git && \ 
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
 
COPY . .  
ENV FLASK_APP=lda_bot.py

CMD ["flask", "run", "--host=0.0.0.0", "--port=8080"]
