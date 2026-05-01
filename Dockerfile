FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data /app/logs /app/downloads && \
    useradd --create-home botuser && \
    chown -R botuser:botuser /app

USER botuser

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "bot.main"]
