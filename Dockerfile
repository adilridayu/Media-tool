FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py index.html ./
RUN useradd -m app && mkdir /data && chown app /data
USER app
ENV WORKDIR=/data TRUST_PROXY=0
EXPOSE 5000
HEALTHCHECK --interval=30s CMD curl -fs http://127.0.0.1:5000/healthz || exit 1
# 1 worker + banyak thread: status job disimpan di memori proses
CMD ["gunicorn","-w","1","--threads","16","-t","0","-b","0.0.0.0:5000","app:app"]
