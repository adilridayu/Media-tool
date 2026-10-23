"""
Media Toolkit (production) — Downloader dari link + Video -> MP3
Fitur: antrean job, progress real-time, batalkan job, auto-cleanup, batas
per-IP, proteksi SSRF, basic-auth opsional, security headers.

Jalankan produksi (WAJIB 1 worker, banyak thread — job disimpan di memori):
  gunicorn -w 1 --threads 16 -t 0 -b 0.0.0.0:5000 app:app
"""
import ipaddress
import logging
import os
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import yt_dlp
from flask import Flask, Response, jsonify, request, send_file
from werkzeug.middleware.proxy_fix import ProxyFix

MB = 1024 * 1024
WORKERS = int(os.environ.get("MAX_WORKERS", 3))          # job paralel
MAX_PER_IP = int(os.environ.get("MAX_JOBS_PER_IP", 2))   # job aktif per IP
TTL = int(os.environ.get("FILE_TTL", 1800))              # detik file disimpan
MAX_MB = int(os.environ.get("MAX_FILE_MB", 2048))        # batas upload & unduhan
MAX_DURATION = int(os.environ.get("MAX_DURATION", 10800))  # detik (3 jam)
WORKDIR = os.environ.get("WORKDIR", os.path.join(tempfile.gettempdir(), "media-toolkit"))
AUTH = (os.environ.get("AUTH_USER"), os.environ.get("AUTH_PASS"))
QUALITIES = {"best", "1080", "720", "480", "360"}
BITRATES = {"128", "192", "256", "320"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("toolkit")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_MB * MB
if os.environ.get("TRUST_PROXY") == "1":  # aktifkan jika di belakang nginx/caddy
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)

with open(os.path.join(os.path.dirname(__file__), "index.html"), encoding="utf-8") as fh:
    INDEX = fh.read()

shutil.rmtree(WORKDIR, ignore_errors=True)
os.makedirs(WORKDIR, exist_ok=True)

JOBS, LOCK = {}, threading.Lock()
POOL = ThreadPoolExecutor(WORKERS)
PUBLIC = ("id", "kind", "status", "stage", "progress", "speed", "eta", "title", "name", "size", "error")


class Cancelled(Exception):
    pass


# ---------- util ----------
def safe_url(u):
    """Tolak non-http(s) dan host yang mengarah ke IP privat/loopback (SSRF)."""
    p = urlparse(u or "")
    if p.scheme not in ("http", "https") or not p.hostname:
        return False
    try:
        infos = socket.getaddrinfo(p.hostname, None)
    except OSError:
        return False
    return all(ipaddress.ip_address(i[4][0]).is_global for i in infos)


def clean_err(e):
    s = re.sub(r"\x1b\[[0-9;]*m", "", str(e)).replace("ERROR: ", "")
    return s.strip()[:300] or "Terjadi kesalahan."


def clean_name(n):
    return re.sub(r'[\\/:*?"<>|\x00-\x1f]+', "_", n).strip()[:100] or "file"


def new_job(kind):
    jid, ip = uuid.uuid4().hex, request.remote_addr
    with LOCK:
        active = sum(1 for x in JOBS.values() if x["ip"] == ip and x["status"] in ("queued", "running"))
        if active >= MAX_PER_IP:
            return None
        d = os.path.join(WORKDIR, jid)
        os.makedirs(d)
        JOBS[jid] = dict(id=jid, kind=kind, ip=ip, dir=d, status="queued", stage="Dalam antrean",
                         progress=0.0, speed=None, eta=None, title="", file=None, name=None,
                         size=None, error=None, created=time.time(), finished=None, cancel=False)
        return JOBS[jid]


def execute(j, fn, *args):
    j.update(status="running", stage="Memulai")
    try:
        path = fn(j, *args)
        j.update(status="done", stage="Selesai", progress=100.0, speed=None, eta=None, file=path,
                 name=os.path.basename(path), size=os.path.getsize(path))
    except Cancelled:
        j.update(status="cancelled", stage="Dibatalkan")
    except FileNotFoundError:
        j.update(status="error", error="ffmpeg belum terpasang di server.")
    except Exception as e:  # noqa: BLE001
        log.warning("job %s gagal: %s", j["id"][:8], e)
        j.update(status="error", error=clean_err(e))
    finally:
        j["finished"] = time.time()
        if j["status"] != "done":
            shutil.rmtree(j["dir"], ignore_errors=True)


def janitor():
    while True:
        time.sleep(60)
        now = time.time()
        with LOCK:
            old = [k for k, v in JOBS.items() if v["finished"] and now - v["finished"] > TTL]
            for k in old:
                shutil.rmtree(JOBS[k]["dir"], ignore_errors=True)
                del JOBS[k]


threading.Thread(target=janitor, daemon=True).start()


# ---------- workers ----------
def run_download(j, url, mode, quality):
    st = {"idx": 0, "last": None}

    def hook(d):
        if j["cancel"]:
            raise Cancelled()
        info = d.get("info_dict", {})
        j["title"] = info.get("title") or j["title"]
        if d["status"] == "downloading":
            n = len(info.get("requested_formats") or []) or 1
            if st["last"] not in (None, d.get("filename")):
                st["idx"] += 1
            st["last"] = d.get("filename")
            tot = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            frac = d["downloaded_bytes"] / tot if tot else 0
            j.update(stage="Mengunduh" + (f" ({min(st['idx'] + 1, n)}/{n})" if n > 1 else ""),
                     progress=min(95.0, (st["idx"] + frac) / n * 95), speed=d.get("speed"), eta=d.get("eta"))

    def pp(d):
        if d["status"] == "started":
            j.update(stage="Memproses (ffmpeg)", progress=97.0, speed=None, eta=None)

    opts = {
        "outtmpl": os.path.join(j["dir"], "%(title).80s.%(ext)s"), "noplaylist": True,
        "quiet": True, "no_warnings": True, "progress_hooks": [hook], "postprocessor_hooks": [pp],
        "max_filesize": MAX_MB * MB, "socket_timeout": 20, "retries": 3,
        "concurrent_fragment_downloads": 4,
        "match_filter": yt_dlp.utils.match_filter_func(f"duration <=? {MAX_DURATION}"),
    }
    if mode == "mp3":
        opts.update(format="bestaudio/best", postprocessors=[{
            "key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": quality}])
        ext = ".mp3"
    else:
        h = f"[height<={quality}]" if quality != "best" else ""
        opts.update(format=f"bv*{h}+ba/b{h}/b", merge_output_format="mp4")
        ext = ".mp4"
    with yt_dlp.YoutubeDL(opts) as y:
        y.download([url])
    files = [f for f in os.listdir(j["dir"]) if f.endswith(ext)]
    if not files:
        raise RuntimeError("Video ditolak (terlalu panjang/besar) atau tidak bisa diunduh.")
    return os.path.join(j["dir"], files[0])


def run_convert(j, src, base, bitrate):
    out = os.path.join(j["dir"], base + ".mp3")
    j.update(stage="Mengonversi", title=base)
    try:
        dur = float(subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", src],
            capture_output=True, text=True, timeout=30).stdout.strip() or 0)
    except (ValueError, subprocess.SubprocessError):
        dur = 0
    p = subprocess.Popen(
        ["ffmpeg", "-y", "-i", src, "-vn", "-acodec", "libmp3lame", "-b:a", f"{bitrate}k",
         "-progress", "pipe:1", "-nostats", out],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    for line in p.stdout:
        if j["cancel"]:
            p.kill()
            raise Cancelled()
        if dur and line.startswith("out_time_us="):
            try:
                j["progress"] = min(99.0, int(line.split("=")[1]) / 1e6 / dur * 100)
            except ValueError:
                pass
    if p.wait() != 0 or not os.path.exists(out):
        raise RuntimeError("Konversi gagal — file bukan video/audio yang valid.")
    os.remove(src)
    return out


# ---------- hooks ----------
@app.before_request
def guard():
    if AUTH[0] and request.path != "/healthz":
        a = request.authorization
        if not a or a.username != AUTH[0] or a.password != AUTH[1]:
            return Response("Login diperlukan", 401, {"WWW-Authenticate": 'Basic realm="Media Toolkit"'})


@app.after_request
def headers(r):
    r.headers.update({
        "X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY", "Referrer-Policy": "no-referrer",
        "Content-Security-Policy": "default-src 'self'; img-src 'self' https: data:; "
                                   "style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'"})
    return r


# ---------- routes ----------
@app.get("/")
def index():
    return Response(INDEX, mimetype="text/html")


@app.get("/healthz")
def healthz():
    return "ok"


@app.post("/api/info")
def info():
    url = (request.get_json(silent=True) or {}).get("url", "").strip()
    if not safe_url(url):
        return jsonify(error="Link tidak valid."), 400
    try:
        with yt_dlp.YoutubeDL({"quiet": True, "noplaylist": True, "socket_timeout": 15}) as y:
            d = y.extract_info(url, download=False)
        return jsonify(title=d.get("title"), thumbnail=d.get("thumbnail"),
                       uploader=d.get("uploader"), duration=d.get("duration"))
    except Exception as e:  # noqa: BLE001
        return jsonify(error=clean_err(e)), 400


@app.post("/api/download")
def api_download():
    d = request.get_json(silent=True) or {}
    url, mode, q = d.get("url", "").strip(), d.get("mode", "mp4"), str(d.get("quality", "best"))
    if not safe_url(url):
        return jsonify(error="Link tidak valid atau tidak diizinkan."), 400
    if mode not in ("mp4", "mp3") or q not in (QUALITIES | BITRATES):
        return jsonify(error="Parameter tidak valid."), 400
    j = new_job("download")
    if not j:
        return jsonify(error=f"Maksimal {MAX_PER_IP} proses aktif. Tunggu salah satu selesai."), 429
    POOL.submit(execute, j, run_download, url, mode, q)
    return jsonify(id=j["id"])


@app.post("/api/convert")
def api_convert():
    f, br = request.files.get("file"), request.form.get("bitrate", "192")
    if not f or not f.filename or br not in BITRATES:
        return jsonify(error="File atau bitrate tidak valid."), 400
    j = new_job("convert")
    if not j:
        return jsonify(error=f"Maksimal {MAX_PER_IP} proses aktif. Tunggu salah satu selesai."), 429
    base = clean_name(os.path.splitext(f.filename)[0])
    src = os.path.join(j["dir"], "input" + re.sub(r"[^.\w]", "", os.path.splitext(f.filename)[1])[:8])
    f.save(src)
    POOL.submit(execute, j, run_convert, src, base, br)
    return jsonify(id=j["id"])


def get_job(jid):
    return JOBS.get(jid)


@app.get("/api/jobs/<jid>")
def job_status(jid):
    j = get_job(jid)
    return (jsonify({k: j[k] for k in PUBLIC}), 200) if j else (jsonify(error="Job tidak ditemukan."), 404)


@app.post("/api/jobs/<jid>/cancel")
def job_cancel(jid):
    j = get_job(jid)
    if not j:
        return jsonify(error="Job tidak ditemukan."), 404
    j["cancel"] = True
    if j["status"] == "queued":
        j.update(status="cancelled", stage="Dibatalkan", finished=time.time())
        shutil.rmtree(j["dir"], ignore_errors=True)
    return jsonify(ok=True)


@app.get("/api/jobs/<jid>/file")
def job_file(jid):
    j = get_job(jid)
    if not j or j["status"] != "done" or not os.path.exists(j["file"]):
        return jsonify(error="File tidak tersedia / sudah kedaluwarsa."), 404
    return send_file(j["file"], as_attachment=True, download_name=j["name"])


if __name__ == "__main__":  # hanya untuk pengembangan lokal
    app.run(host="127.0.0.1", port=5000, threaded=True)
