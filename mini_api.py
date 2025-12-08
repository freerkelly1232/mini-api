#!/usr/bin/env python3
"""
MINI API - ULTIMATE SERVER HOPPER HELPER
- Deep pagination (separate cursor pool)
- Both Asc + Desc fetching  
- Sends unique servers to Main API
- Runs alongside Main API for 2x coverage
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

# Gentle fetching (proxy friendly)
FETCH_THREADS = 8
FETCH_INTERVAL = 2
PAGES_PER_CYCLE = 40
SEND_BATCH_SIZE = 500
MAX_CURSORS = 800

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
        self.pages = 0
        self.last_rate = 0
        self.last_added_rate = 0

stats = Stats()

# Cursor storage for deep pagination
cursors_asc = deque(maxlen=MAX_CURSORS)
cursors_desc = deque(maxlen=MAX_CURSORS)
cursor_lock = threading.Lock()

# Local dedup to avoid sending same servers repeatedly
local_seen = set()
local_seen_lock = threading.Lock()
MAX_LOCAL_SEEN = 100000

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
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json'
            }
        )
        
        if resp.status_code == 200:
            data = resp.json()
            return data.get('data', []), data.get('nextPageCursor'), sort_order
        elif resp.status_code == 429:
            time.sleep(1)
            return [], None, sort_order
        return [], None, sort_order
        
    except Exception as e:
        with stats.lock:
            stats.errors += 1
        return [], None, sort_order

def fetch_cycle():
    global local_seen
    
    fetches = []
    
    # Fresh fetches (both directions)
    fetches.append((None, 'Asc'))
    fetches.append((None, 'Desc'))
    
    # Sample from stored cursors at different depths
    with cursor_lock:
        asc_list = list(cursors_asc)
        desc_list = list(cursors_desc)
        
        # Evenly sample from Asc cursors
        if asc_list:
            step = max(1, len(asc_list) // 50)
            for i in range(0, len(asc_list), step):
                fetches.append((asc_list[i], 'Asc'))
        
        # Evenly sample from Desc cursors
        if desc_list:
            step = max(1, len(desc_list) // 50)
            for i in range(0, len(desc_list), step):
                fetches.append((desc_list[i], 'Desc'))
    
    # Limit fetches per cycle
    fetches = fetches[:PAGES_PER_CYCLE]
    
    all_servers = []
    new_cursors_asc = []
    new_cursors_desc = []
    pages_fetched = 0
    
    # Execute in parallel
    with ThreadPoolExecutor(max_workers=FETCH_THREADS) as executor:
        futures = {executor.submit(fetch_page, c, s): (c, s) for c, s in fetches}
        
        for future in as_completed(futures):
            try:
                servers, next_cursor, sort_order = future.result()
                pages_fetched += 1
                
                for s in servers:
                    sid = s.get('id')
                    players = s.get('playing', 0)
                    
                    if not sid:
                        continue
                    
                    # Skip if we've seen it locally (avoid re-sending)
                    with local_seen_lock:
                        if sid in local_seen:
                            continue
                        local_seen.add(sid)
                        
                        # Trim local seen
                        if len(local_seen) > MAX_LOCAL_SEEN:
                            local_seen = set(list(local_seen)[-50000:])
                    
                    all_servers.append({'id': sid, 'players': players})
                
                if next_cursor:
                    if sort_order == 'Asc':
                        new_cursors_asc.append(next_cursor)
                    else:
                        new_cursors_desc.append(next_cursor)
                        
            except Exception as e:
                pass
    
    # Store new cursors
    with cursor_lock:
        for c in new_cursors_asc:
            cursors_asc.append(c)
        for c in new_cursors_desc:
            cursors_desc.append(c)
    
    with stats.lock:
        stats.fetched += len(all_servers)
        stats.pages += pages_fetched
    
    return all_servers

def send_to_main(servers):
    if not servers:
        return 0
    
    total_added = 0
    
    # Send in batches
    for i in range(0, len(servers), SEND_BATCH_SIZE):
        batch = servers[i:i + SEND_BATCH_SIZE]
        try:
            resp = requests.post(
                f"{MAIN_API_URL}/add-pool",
                json={'servers': batch, 'source': 'mini-api'},
                timeout=15
            )
            if resp.ok:
                result = resp.json()
                total_added += result.get('added', 0)
        except Exception as e:
            log.error(f"Send error: {e}")
            with stats.lock:
                stats.errors += 1
    
    return total_added

def run_loop():
    last_log = time.time()
    servers_this_min = 0
    added_this_min = 0
    pages_this_min = 0
    
    while True:
        try:
            # Fetch servers
            servers = fetch_cycle()
            servers_this_min += len(servers)
            
            with stats.lock:
                pages_this_min = stats.pages
            
            # Send to main API
            if servers:
                added = send_to_main(servers)
                added_this_min += added
                
                with stats.lock:
                    stats.sent += len(servers)
                    stats.added += added
            
            # Log every minute
            if time.time() - last_log >= 60:
                with stats.lock:
                    stats.last_rate = servers_this_min
                    stats.last_added_rate = added_this_min
                
                with cursor_lock:
                    cursor_count = len(cursors_asc) + len(cursors_desc)
                
                log.info(f"[MINI] Fetched: {servers_this_min}/min | Added: {added_this_min}/min | Cursors: {cursor_count} | Total sent: {stats.sent}")
                
                servers_this_min = 0
                added_this_min = 0
                pages_this_min = 0
                last_log = time.time()
                
        except Exception as e:
            log.error(f"Loop error: {e}")
        
        time.sleep(FETCH_INTERVAL)

# ==================== ENDPOINTS ====================
@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'service': 'Mini API - Server Hopper Helper',
        'status': 'running',
        'main_api': MAIN_API_URL
    })

@app.route('/status', methods=['GET'])
def status():
    with stats.lock:
        with cursor_lock:
            with local_seen_lock:
                return jsonify({
                    'status': 'running',
                    'fetched_total': stats.fetched,
                    'sent_total': stats.sent,
                    'added_total': stats.added,
                    'errors': stats.errors,
                    'rate_fetched': stats.last_rate,
                    'rate_added': stats.last_added_rate,
                    'cursors_asc': len(cursors_asc),
                    'cursors_desc': len(cursors_desc),
                    'cursors_total': len(cursors_asc) + len(cursors_desc),
                    'local_seen': len(local_seen),
                    'main_api': MAIN_API_URL
                })

@app.route('/health', methods=['GET'])
def health():
    with stats.lock:
        return jsonify({
            'status': 'ok',
            'sent': stats.sent,
            'added': stats.added
        })

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8001))
    
    log.info("=" * 60)
    log.info("  MINI API - SERVER HOPPER HELPER")
    log.info("=" * 60)
    log.info(f"  Port: {port}")
    log.info(f"  Main API: {MAIN_API_URL}")
    log.info(f"  Fetch Threads: {FETCH_THREADS}")
    log.info(f"  Max Cursors: {MAX_CURSORS}")
    log.info("=" * 60)
    
    # Start fetcher loop
    threading.Thread(target=run_loop, daemon=True).start()
    
    # Start Flask
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)

