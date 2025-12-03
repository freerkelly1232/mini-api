#!/usr/bin/env python3
"""
OPTIMIZED MINI API - Parallel Server Fetcher
Features:
- Parallel fetching across multiple proxies
- Automatic proxy rotation on rate limits
- Batch sending to main API
- Continuous scanning with no gaps
"""

import os
import time
import logging
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify
import requests

# ==================== CONFIG ====================
logging.basicConfig(
    level=logging.INFO,
    format='[MINI %(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# Roblox Config
GAME_ID = os.environ.get("GAME_ID", "109983668079237")
BASE_URL = f"https://games.roblox.com/v1/games/{GAME_ID}/servers/Public"

# Main API endpoint
MAIN_API_URL = os.environ.get("MAIN_API_URL", "https://main-api-production-0871.up.railway.app/add-pool")

# Performance Tuning
FETCH_WORKERS = 4                   # Parallel workers per cycle
PAGES_PER_WORKER = 25               # Each worker fetches this many pages
SEND_INTERVAL = 3                   # Seconds between send cycles
REQUEST_TIMEOUT = 8
BATCH_SIZE = 500                    # Send to main API in batches

# ==================== PROXIES ====================
# Add your proxies here - more proxies = faster fetching
PROXIES = [
    # Format: "http://user:pass@host:port" or "http://host:port"
    "http://proxy-e5a1ntzmrlr3_area-US:Ol43jGdsIuPUNacc@us.naproxy.net:1000",
    # Add more proxies for better performance:
    # "http://proxy2:port",
    # "http://proxy3:port",
    None,  # Direct connection (no proxy)
]

# ==================== STATE ====================
class FetchStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.total_fetched = 0
        self.total_sent = 0
        self.last_cycle_time = 0
        self.errors = 0
        self.rate_limits = 0

stats = FetchStats()
pending_servers = deque(maxlen=50000)  # Buffer for servers to send

# ==================== FETCHING ====================
def get_proxy(index):
    """Get proxy by index with rotation"""
    if not PROXIES:
        return None
    proxy = PROXIES[index % len(PROXIES)]
    if proxy:
        return {"http": proxy, "https": proxy}
    return None

def fetch_pages_sequential(worker_id, start_cursor=None, max_pages=25):
    """Fetch multiple pages sequentially with one proxy"""
    servers = []
    cursor = start_cursor
    proxy_idx = worker_id
    
    for page in range(max_pages):
        try:
            proxies = get_proxy(proxy_idx)
            params = {'sortOrder': 'Asc', 'limit': 100}
            if cursor:
                params['cursor'] = cursor
            
            resp = requests.get(
                BASE_URL, 
                params=params, 
                proxies=proxies, 
                timeout=REQUEST_TIMEOUT
            )
            
            if resp.status_code == 429:
                log.warning(f"[Worker {worker_id}] Rate limited, rotating proxy...")
                with stats.lock:
                    stats.rate_limits += 1
                proxy_idx += 1
                time.sleep(1)
                continue
            
            resp.raise_for_status()
            data = resp.json()
            
            page_servers = data.get('data', [])
            for s in page_servers:
                if s.get('id'):
                    servers.append({
                        'id': s['id'],
                        'players': s.get('playing', 0)
                    })
            
            cursor = data.get('nextPageCursor')
            if not cursor:
                break
            
            time.sleep(0.1)  # Small delay between pages
            
        except requests.exceptions.RequestException as e:
            log.debug(f"[Worker {worker_id}] Error: {e}")
            with stats.lock:
                stats.errors += 1
            proxy_idx += 1
            time.sleep(0.5)
    
    return servers, cursor

def parallel_fetch():
    """Run multiple fetch workers in parallel"""
    all_servers = []
    
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as executor:
        # Start workers with different starting points
        futures = []
        for i in range(FETCH_WORKERS):
            future = executor.submit(fetch_pages_sequential, i, None, PAGES_PER_WORKER)
            futures.append(future)
        
        for future in as_completed(futures):
            try:
                servers, _ = future.result()
                all_servers.extend(servers)
            except Exception as e:
                log.error(f"Worker error: {e}")
    
    return all_servers

def send_to_main(servers):
    """Send servers to main API in batches"""
    if not servers:
        return 0
    
    total_added = 0
    
    # Send in batches
    for i in range(0, len(servers), BATCH_SIZE):
        batch = servers[i:i + BATCH_SIZE]
        try:
            # Send just job IDs for efficiency
            payload = {"servers": [s['id'] if isinstance(s, dict) else s for s in batch]}
            
            resp = requests.post(
                MAIN_API_URL, 
                json=payload, 
                timeout=REQUEST_TIMEOUT
            )
            
            if resp.ok:
                added = resp.json().get('added', 0)
                total_added += added
            else:
                log.warning(f"Main API returned {resp.status_code}")
                
        except Exception as e:
            log.error(f"Error sending to main: {e}")
    
    return total_added

# ==================== MAIN LOOP ====================
def fetch_and_send_loop():
    """Continuous fetch and send loop"""
    while True:
        cycle_start = time.time()
        
        try:
            # Fetch from Roblox
            servers = parallel_fetch()
            
            with stats.lock:
                stats.total_fetched += len(servers)
            
            if servers:
                # Add to pending queue
                for s in servers:
                    pending_servers.append(s)
                
                # Send to main API
                to_send = list(pending_servers)
                pending_servers.clear()
                
                added = send_to_main(to_send)
                
                with stats.lock:
                    stats.total_sent += added
                    stats.last_cycle_time = time.time() - cycle_start
                
                log.info(f"✅ Fetched {len(servers)} | Sent {len(to_send)} | Added {added} | Time {stats.last_cycle_time:.1f}s")
            else:
                log.warning("No servers fetched this cycle")
                
        except Exception as e:
            log.exception(f"Cycle error: {e}")
        
        # Wait for next cycle
        elapsed = time.time() - cycle_start
        sleep_time = max(0, SEND_INTERVAL - elapsed)
        time.sleep(sleep_time)

# ==================== ENDPOINTS ====================
@app.route('/', methods=['GET'])
def home():
    """Status endpoint"""
    with stats.lock:
        return jsonify({
            'status': 'running',
            'total_fetched': stats.total_fetched,
            'total_sent': stats.total_sent,
            'pending': len(pending_servers),
            'last_cycle_time': round(stats.last_cycle_time, 2),
            'errors': stats.errors,
            'rate_limits': stats.rate_limits,
            'workers': FETCH_WORKERS,
            'proxies': len([p for p in PROXIES if p])
        })

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8001))
    
    log.info(f"🚀 Optimized Mini API starting on port {port}")
    log.info(f"   Workers: {FETCH_WORKERS}, Proxies: {len(PROXIES)}")
    log.info(f"   Main API: {MAIN_API_URL}")
    
    # Start fetch loop in background
    threading.Thread(target=fetch_and_send_loop, daemon=True).start()
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
