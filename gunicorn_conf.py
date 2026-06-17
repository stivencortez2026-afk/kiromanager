"""Gunicorn config para Render.com."""
import os
import multiprocessing

bind = f"0.0.0.0:{os.getenv('PORT', '8000')}"
worker_class = "uvicorn.workers.UvicornWorker"
workers = int(os.getenv("WEB_CONCURRENCY", min(4, multiprocessing.cpu_count() + 1)))
timeout = int(os.getenv("GUNICORN_TIMEOUT", "300"))
graceful_timeout = 120
keepalive = 5
preload_app = True
accesslog = "-"
errorlog = "-"
loglevel = os.getenv("LOG_LEVEL", "info")
max_requests = 1000
max_requests_jitter = 50
