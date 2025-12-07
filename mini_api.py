#!/usr/bin/env python3
"""
MINI API - Deep Pagination Helper
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

# NAProxy
PROXY_HOST = "us.naproxy.net"
PROXY_PORT = "1000"
PROXY_USER = "proxy-e5a1ntzmrlr3_area-US"
PROXY_PASS = "Ol43jGdsIuPUNacc"

# Deep fetching
FETCH_THREADS = 20
FETCH_INTERVAL = 1
PAGES_PER_CYCLE = 150
SEND_BATCH_SIZE = 500
MAX_CURSORS = 2000

logging.basicConfig(
    level=logging.INFO,
    format='[MINI %(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ==================== STATE ====================
class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.fetched = 0
        self.sent = 0
        self.added = 0
        self.errors = 0
        self.last_rate = 0

stats = Stats()
cursors_asc = deque(maxlen=MAX_CURSORS)
cursors_desc = deque(maxlen=MAX_CURSORS)
cursor_lock = threading.Lock()

# ==================== PROXY ====================
def get_proxy():
    session_id = f"mini{random.randint(100000, 999999)}"
    proxy_url = f"http://{PROXY_USER}_session-{session_id}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    return {'http': proxy_url, 'https': proxy_url}

# ==================== FETCHING ====================
def fetch_page(cursor=None, sort_order='Desc'):
    try:
        url = f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public"
        params = {'sortOrder': sort_order, 'limit': 100}
        if cursor:
            params['cursor'] = cursor
        
        resp = requests.get(
            url,
            params=params,
            proxies=get_proxy(),
            timeout=10,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
        )
        
        if resp.status_code == 200:
            data = resp.json()
            return data.get('data', []), data.get('nextPageCursor'), sort_order
        return [], None, sort_order
        
    except:
        with stats.lock:
            stats.errors += 1
        return [], None, sort_order

def fetch_cycle():
    fetches = []
    
    fetches.append((None, 'Asc'))
    fetches.append((None, 'Desc'))
    
    with cursor_lock:
        asc_list = list(cursors_asc)
        desc_list = list(cursors_desc)
        
        step = max(1, len(asc_list) // 40)
        for i in range(0, len(asc_list), step):
            fetches.append((asc_list[i], 'Asc'))
        
        step = max(1, len(desc_list) // 40)
        for i in range(0, len(desc_list), step):
            fetches.append((desc_list[i], 'Desc'))
    
    fetches = fetches[:PAGES_PER_CYCLE]
    
    all_servers = []
    seen = set()
    new_asc = []
    new_desc = []
    
    with ThreadPoolExecutor(max_workers=FETCH_THREADS) as executor:
        futures = {executor.submit(fetch_page, c, s): (c, s) for c, s in fetches}
        
        for future in as_completed(futures):
            try:
                servers, next_cursor, sort_order = future.result()
                
                for s in servers:
                    sid = s.get('id')
                    if sid and sid not in seen:
                        seen.add(sid)
                        all_servers.append({'id': sid, 'players': s.get('playing', 0)})
                
                if next_cursor:
                    if sort_order == 'Asc':
                        new_asc.append(next_cursor)
                    else:
                        new_desc.append(next_cursor)
                        
            except:
                pass
    
    with cursor_lock:
        for c in new_asc:
            cursors_asc.append(c)
        for c in new_desc:
            cursors_desc.append(c)
    
    return all_servers

def send_to_main(servers):
    if not servers:
        return 0
    
    total_added = 0
    
    for i in range(0, len(servers), SEND_BATCH_SIZE):
        batch = servers[i:i + SEND_BATCH_SIZE]
        try:
            resp = requests.post(
                f"{MAIN_API_URL}/add-pool",
                json={'servers': batch, 'source': 'mini-api'},
                timeout=10
            )
            if resp.ok:
                total_added += resp.json().get('added', 0)
        except Exception as e:
            log.error(f"Send error: {e}")
    
    return total_added

def run_loop():
    last_log = time.time()
    servers_this_min = 0
    
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
                
                servers_this_min += len(servers)
            
            if time.time() - last_log >= 60:
                with stats.lock:
                    stats.last_rate = servers_this_min
                with cursor_lock:
                    cursor_count = len(cursors_asc) + len(cursors_desc)
                log.info(f"[RATE] {servers_this_min}/min | Cursors: {cursor_count}")
                servers_this_min = 0
                last_log = time.time()
                
        except Exception as e:
            log.error(f"Loop error: {e}")
        
        time.sleep(FETCH_INTERVAL)

# ==================== ENDPOINTS ====================
@app.route('/', methods=['GET'])
@app.route('/status', methods=['GET'])
def status():
    with stats.lock:
        with cursor_lock:
            return jsonify({
                'status': 'running',
                'fetched': stats.fetched,
                'sent': stats.sent,
                'added': stats.added,
                'errors': stats.errors,
                'rate_per_min': stats.last_rate,
                'cursors': len(cursors_asc) + len(cursors_desc)
            })

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8001))
    log.info(f"[STARTUP] Mini API on port {port}")
    threading.Thread(target=run_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
