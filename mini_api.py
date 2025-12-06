#!/usr/bin/env python3
"""
MINI API - Fetches DESCENDING servers
"""

import os
import time
import logging
import threading
import requests
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify
import random

# ==================== CONFIG ====================
PLACE_ID = 109983668079237
MAIN_API_URL = os.environ.get("MAIN_API_URL", "https://main-api-production-0871.up.railway.app")
SORT_ORDER = "Desc"

# NAProxy US (same as main)
PROXY_HOST = "us.naproxy.net"
PROXY_PORT = "1000"
PROXY_USER = "proxy-e5a1ntzmrlr3_area-US"
PROXY_PASS = "Ol43jGdsIuPUNacc"

FETCH_THREADS = 15
FETCH_INTERVAL = 2
CURSORS_PER_CYCLE = 80
SEND_BATCH_SIZE = 500

logging.basicConfig(
    level=logging.INFO,
    format='[MINI %(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.fetched = 0
        self.sent = 0
        self.added = 0
        self.errors = 0
        self.last_rate = 0

stats = Stats()
cursors = deque(maxlen=500)

def get_proxy():
    session_id = f"mini{random.randint(100000, 999999)}"
    proxy_url = f"http://{PROXY_USER}_session-{session_id}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    return {'http': proxy_url, 'https': proxy_url}

def fetch_page(cursor=None):
    try:
        url = f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public"
