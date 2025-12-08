#!/usr/bin/env python3
"""
MINI API - Additional Server Fetcher
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

# NAProxy - US endpoint
PROXY_HOST = "us.naproxy.net"
PROXY_PORT = "1000"
PROXY_USER = "proxy-e5a1ntzmrlr3_area-US"
PROXY_PASS = "Ol43jGdsIuPUNacc"

# Reduced fetching (proxy friendly)
FETCH_THREADS = 6
FETCH_INTERVAL = 2
PAGES_PER_CYCLE = 30
SEND_BATCH_SIZE = 500

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

# Deep cursor storage for both directions
cursors_asc = deque(maxlen=1500)
cursors_desc = deque(maxlen=1500)
cursor_lock = threading.Lock()

# ==================== PROXY ====================
def get_proxy():
    session_id = f"mini{random.randint(100000, 999999)}"
    proxy_url = f"http://{PROXY_USER}_session-{session_id}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    return {'http': proxy_url, 'https': proxy_url}

# ==================== FETCHING ====================
def fetch_page(cursor=None, sort_order='Desc'):
    max_retries = 2
    for attempt in range(max_retries):
        try:
            url = f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public"
            params = {
                'sortOrder': sort_order,
                'limit': 100,
                'excludeFullGames': 'true'
            }
            if cursor:
                params['cursor'] = cursor
            
            resp = requests.get(
                url,
                params=params,
                proxies=get_proxy(),
                timeout=15,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
            )
            
            if resp.status_code == 200:
                data = resp.json()
                return data.get('data', []), data.get('nextPageCursor'), sort_order
            elif resp.status_code == 429:
                log.warning("[RATELIMIT] 429 from Roblox")
                time.sleep(2)
                return [], None, sort_order
            else:
                log.warning(f"[FETCH] Status {resp.status_code}")
                return [], None, sort_order
            
        except requests.exceptions.SSLError as e:
            log.error(f"[SSL ERROR] {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            with stats.lock:
                stats.errors += 1
            return [], None, sort_order
        except requests.exceptions.ConnectionError as e:
            log.error(f"[CONN ERROR] {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            with stats.lock:
                stats.errors += 1
            return [], None, sort_order
        except requests.exceptions.ProxyError as e:
            log.error(f"[PROXY ERROR] {e}")
            with stats.lock:
                stats.errors += 1
            return [], None, sort_order
        except Exception as e:
            log.error(f"[ERROR] {type(e).__name__}: {e}")
            with stats.lock:
                stats.errors += 1
            return [], None, sort_order
    
    return [], None, sort_order

def fetch_cycle():
    global cursors_asc, cursors_desc
    
    fetches = []
    
    # Start fresh from both ends
    fetches.append((None, 'Asc'))
    fetches.append((None, 'Desc'))
    
    # Prioritize DEEP cursors (closer to middle where 5-7 player servers are)
    with cursor_lock:
        asc_list = list(cursors_asc)
        desc_list = list(cursors_desc)
        
        # Take deepest cursors (end of list)
        if asc_list:
            deep_asc = asc_list[-min(15, len(asc_list)):]
            for c in deep_asc:
                fetches.append((c, 'Asc'))
        
        if desc_list:
            deep_desc = desc_list[-min(15, len(desc_list)):]
            for c in deep_desc:
                fetches.append((c, 'Desc'))
    
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
                    players = s.get('playing', 0)
                    
                    # Skip servers with less than 5 players
                    if players < 5:
                        continue
                    
                    if sid and sid not in seen:
                        seen.add(sid)
                        all_servers.append({'id': sid, 'players': players})
                
                if next_cursor:
                    if sort_order == 'Asc':
                        new_asc.append(next_cursor)
                    else:
                        new_desc.append(next_cursor)
                        
            except Exception as e:
                log.error(f"[CYCLE] Future error: {e}")
    
    # Store new cursors (go deeper)
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

# ==================== MAIN LOOP ====================
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
                    depth = len(cursors_asc) + len(cursors_desc)
                log.info(f"[RATE] {servers_this_min}/min | Sent: {stats.sent} | Added: {stats.added} | Depth: {depth}")
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
                'depth_asc': len(cursors_asc),
                'depth_desc': len(cursors_desc),
                'depth_total': len(cursors_asc) + len(cursors_desc)
            })

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8001))
    
    log.info(f"[STARTUP] Mini API on port {port}")
    log.info(f"[CONFIG] Threads={FETCH_THREADS} | Main API={MAIN_API_URL}")
    
    threading.Thread(target=run_loop, daemon=True).start()
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
