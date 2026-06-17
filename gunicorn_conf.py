"""
Gunicorn configuration for Render.com deployment.
Otimizado para manter o gateway estável em produção.
"""

import os
import multiprocessing

# Bind
bind = f"0.0.0.0:{os.getenv('PORT', '8000')}"

# Workers - Uvicorn worker class para suportar async
worker_class = "uvicorn.workers.UvicornWorker"

# Número de workers: para um gateway com I/O bound, 2-4 é ideal
# Render free tier: 1 worker. Paid: baseado em CPUs disponíveis.
workers = int(os.getenv("WEB_CONCURRENCY", min(4, multiprocessing.cpu_count() + 1)))

# Timeout generoso para streaming de respostas longas
timeout = int(os.getenv("GUNICORN_TIMEOUT", "300"))
graceful_timeout = 120
keepalive = 5

# Preload app para compartilhar memória entre workers
preload_app = True

# Logging
accesslog = "-"
errorlog = "-"
loglevel = os.getenv("LOG_LEVEL", "info")

# Restart workers periodicamente para evitar memory leaks
max_requests = 1000
max_requests_jitter = 50
