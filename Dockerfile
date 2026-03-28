FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y gcc libc6 libstdc++6 curl && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash syncuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sync/ sync/
COPY schemas/ schemas/
COPY config/ config/
COPY deploy_connectors.py .
COPY main.py .
COPY entrypoint.sh .

RUN chmod +x entrypoint.sh
RUN mkdir -p state logs && chown -R syncuser:syncuser /app

USER syncuser

ENV PYTHONUNBUFFERED=1 TZ=Asia/Ho_Chi_Minh

CMD ["./entrypoint.sh"]