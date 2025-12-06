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

# NAProxy US
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
        params = {'sortOrder': SORT_ORDER, 'limit': 100}
        if cursor:
            params['cursor'] = cursor
        
        resp = requests.get(url, params=params, proxies=get_proxy(), timeout=10,
                           headers={'User-Agent': 'Mozilla/5.0'})
        
        if resp.status_code == 200:
            data = resp.json()
            return data.get('data', []), data.get('nextPageCursor')
        return [], None
    except Exception as e:
        with stats.lock:
            stats.errors += 1
        return [], None

def fetch_cycle():
    cursors_to_use = [None] + list(cursors)[:CURSORS_PER_CYCLE - 1]
    all_servers = []
    new_cursors = []
    
    with ThreadPoolExecutor(max_workers=FETCH_THREADS) as executor:
        futures = {executor.submit(fetch_page, c): c for c in cursors_to_use}
        for future in as_completed(futures):
            try:
                servers, next_cursor = future.result()
                for s in servers:
                    if s.get('id'):
                        all_servers.append({'id': s['id'], 'players': s.get('playing', 0)})
                if next_cursor:
                    new_cursors.append(next_cursor)
            except Exception as e:
                pass
    
    for c in new_cursors:
        cursors.append(c)
    
    return all_servers

def send_to_main(servers):
    if not servers:
        return 0
    
    total_added = 0
    for i in range(0, len(servers), SEND_BATCH_SIZE):
        batch = servers[i:i + SEND_BATCH_SIZE]
        try:
            resp = requests.post(f"{MAIN_API_URL}/add-pool",
                                json={'servers': batch, 'source': 'mini-api'}, timeout=10)
            if resp.ok:
                total_added += resp.json().get('added', 0)
        except Exception as e:
            pass
    
    return total_added

def run_loop():
    last_log = time.time()
    fetched_this_min = 0
    added_this_min = 0
    
    while True:
        try:
            servers = fetch_cycle()
            
            with stats.lock:
                stats.fetched += len(servers)
            
            if servers:
                added = send_to_main(servers)
                with stats.lock:
                    stats.sent += len(servers)
                    stats.added += added
                fetched_this_min += len(servers)
                added_this_min += added
            
            if time.time() - last_log >= 60:
                with stats.lock:
                    stats.last_rate = fetched_this_min
                log.info(f"[RATE] Fetched: {fetched_this_min}/min | Added: {added_this_min}")
                fetched_this_min = 0
                added_this_min = 0
                last_log = time.time()
                
        except Exception as e:
            log.error(f"Error: {e}")
        
        time.sleep(FETCH_INTERVAL)

@app.route('/', methods=['GET'])
@app.route('/status', methods=['GET'])
def status():
    with stats.lock:
        return jsonify({
            'status': 'running',
            'sort_order': SORT_ORDER,
            'fetched': stats.fetched,
            'sent': stats.sent,
            'added': stats.added,
            'errors': stats.errors,
            'rate_per_min': stats.last_rate,
            'cursors': len(cursors)
        })

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8001))
    log.info(f"[STARTUP] Mini API - Port {port} - {SORT_ORDER}")
    threading.Thread(target=run_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
