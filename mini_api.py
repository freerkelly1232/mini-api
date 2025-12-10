#!/usr/bin/env python3
"""
MINI API - UK/Brazil/US Peak Hour Rotation
Fixed for SOCKS5 proxies from NAProxy
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
from datetime import datetime, timezone, timedelta

# ==================== CONFIG ====================
PLACE_ID = 109983668079237
MAIN_API_URL = os.environ.get("MAIN_API_URL", "https://main-api-production-0871.up.railway.app")

# NAProxy API
NAPROXY_BASE = "https://api.naproxy.com/web_v1/ip/get-ip-v3"
NAPROXY_KEY = "b6e6673dfb2404d26759eddd1bdf9d12"

# Proxy settings
PROXY_COUNT = 20
PROXY_LIFE = 30
MIN_PROXIES = 10

# Peak hours
PEAK_START = 17
PEAK_END = 20

# Regions
REGIONS = [
    {"name": "UK", "cc": "GB", "state": "", "utc_offset": 0},
    {"name": "Brazil", "cc": "BR", "state": "", "utc_offset": -3},
    {"name": "US-East", "cc": "US", "state": "NY", "utc_offset": -5},
    {"name": "US-Central", "cc": "US", "state": "TX", "utc_offset": -6},
    {"name": "US-West", "cc": "US", "state": "CA", "utc_offset": -8},
]

# Fetching
FETCH_THREADS = 12
FETCH_INTERVAL = 2
PAGES_PER_CYCLE = 40
SEND_BATCH = 500

logging.basicConfig(
    level=logging.INFO,
    format='[MINI %(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ==================== PEAK FINDER ====================
def get_hour(offset):
    utc = datetime.now(timezone.utc)
    return (utc + timedelta(hours=offset)).hour

def get_peak_regions():
    return [r for r in REGIONS if PEAK_START <= get_hour(r['utc_offset']) < PEAK_END]

# ==================== PROXY POOL ====================
class ProxyPool:
    def __init__(self):
        self.lock = threading.Lock()
        self.proxies = deque()
        self.region = "Starting"
        self.stats = {'fetched': 0, 'used': 0, 'success': 0, 'errors': 0}
        self.last_error = None
        
        threading.Thread(target=self._loop, daemon=True).start()
    
    def _build_url(self, region=None):
        url = f"{NAPROXY_BASE}?app_key={NAPROXY_KEY}&pt=9&ep=&num={PROXY_COUNT}"
        if region:
            url += f"&cc={region.get('cc', '')}&state={region.get('state', '')}"
        else:
            url += "&cc=&state="
        url += f"&city=&life={PROXY_LIFE}&protocol=1&format=txt&lb=%5Cr%5Cn"
        return url
    
    def _fetch(self, region=None):
        name = region['name'] if region else 'Any'
        
        try:
            resp = requests.get(self._build_url(region), timeout=15)
            text = resp.text.strip()
            
            if 'allow list' in text.lower() or text.startswith('{') or not text:
                return 0
            
            lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
            expire = time.time() + PROXY_LIFE
            count = 0
            
            with self.lock:
                for line in lines:
                    line = line.strip()
                    if line and ':' in line and not line.startswith('{'):
                        # SOCKS5 proxy
                        self.proxies.append((f"socks5://{line}", expire))
                        count += 1
                
                if count > 0:
                    self.stats['fetched'] += count
                    self.region = name
                    self.last_error = None
            
            if count > 0:
                log.info(f"[PROXY] +{count} from {name} (pool: {len(self.proxies)})")
            
            return count
        except Exception as e:
            self.last_error = str(e)
            return 0
    
    def _loop(self):
        while True:
            try:
                now = time.time()
                with self.lock:
                    self.proxies = deque(p for p in self.proxies if p[1] > now)
                    size = len(self.proxies)
                
                if size < MIN_PROXIES:
                    peak = get_peak_regions()
                    region = random.choice(peak) if peak else random.choice(REGIONS)
                    self._fetch(region)
                
                time.sleep(5)
            except:
                time.sleep(2)
    
    def get(self):
        with self.lock:
            now = time.time()
            self.proxies = deque(p for p in self.proxies if p[1] > now)
            
            if self.proxies:
                url, exp = random.choice(list(self.proxies))
                if exp - now > 5:
                    self.stats['used'] += 1
                    return {'http': url, 'https': url}, url
        
        peak = get_peak_regions()
        self._fetch(random.choice(peak) if peak else random.choice(REGIONS))
        
        with self.lock:
            if self.proxies:
                url, _ = self.proxies[0]
                self.stats['used'] += 1
                return {'http': url, 'https': url}, url
        return None, None
    
    def success(self):
        with self.lock:
            self.stats['success'] += 1
    
    def error(self, url=None):
        with self.lock:
            self.stats['errors'] += 1
            if url:
                self.proxies = deque(p for p in self.proxies if p[0] != url)
    
    def get_stats(self):
        with self.lock:
            total = self.stats['success'] + self.stats['errors']
            return {
                'pool': len(self.proxies),
                'region': self.region,
                'success_rate': round(self.stats['success'] / max(1, total) * 100, 1),
                'last_error': self.last_error
            }

proxies = ProxyPool()

# ==================== STATS ====================
stats = {'fetched': 0, 'sent': 0, 'added': 0, 'rate': 0, 'fetch_errors': 0}
cursors_asc = deque(maxlen=2000)
cursors_desc = deque(maxlen=2000)
cursor_lock = threading.Lock()

# ==================== FETCHING ====================
def fetch_page(cursor=None, sort='Desc'):
    proxy, url = proxies.get()
    if not proxy:
        return [], None
    
    try:
        params = {'sortOrder': sort, 'limit': 100, 'excludeFullGames': 'true'}
        if cursor:
            params['cursor'] = cursor
        
        resp = requests.get(
            f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public",
            params=params,
            proxies=proxy,
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        
        if resp.status_code == 200:
            data = resp.json()
            proxies.success()
            return data.get('data', []), data.get('nextPageCursor')
        else:
            proxies.error(url)
            stats['fetch_errors'] += 1
    except Exception as e:
        proxies.error(url)
        stats['fetch_errors'] += 1
    
    return [], None

def fetch_cycle():
    fetches = [(None, 'Asc'), (None, 'Desc')]
    
    with cursor_lock:
        for c in list(cursors_asc)[-25:]:
            fetches.append((c, 'Asc'))
        for c in list(cursors_desc)[-25:]:
            fetches.append((c, 'Desc'))
    
    fetches = fetches[:PAGES_PER_CYCLE]
    
    servers = []
    seen = set()
    new_asc, new_desc = [], []
    
    with ThreadPoolExecutor(max_workers=FETCH_THREADS) as ex:
        futures = {ex.submit(fetch_page, c, s): s for c, s in fetches}
        
        for f in as_completed(futures):
            try:
                data, cursor = f.result()
                for s in data:
                    sid = s.get('id')
                    players = s.get('playing', 0)
                    if players >= 5 and sid and sid not in seen:
                        seen.add(sid)
                        servers.append({'id': sid, 'players': players})
                
                if cursor:
                    (new_asc if futures[f] == 'Asc' else new_desc).append(cursor)
            except:
                pass
    
    with cursor_lock:
        cursors_asc.extend(new_asc)
        cursors_desc.extend(new_desc)
    
    return servers

def send_to_main(servers):
    if not servers:
        return 0
    
    total = 0
    for i in range(0, len(servers), SEND_BATCH):
        batch = servers[i:i + SEND_BATCH]
        try:
            resp = requests.post(f"{MAIN_API_URL}/add-pool", json={'servers': batch, 'source': 'mini'}, timeout=10)
            if resp.ok:
                total += resp.json().get('added', 0)
        except:
            pass
    return total

# ==================== MAIN LOOP ====================
def run():
    global stats
    last_log = time.time()
    this_min = 0
    errors_this_min = 0
    
    while True:
        try:
            servers = fetch_cycle()
            stats['fetched'] += len(servers)
            
            if servers:
                added = send_to_main(servers)
                stats['sent'] += len(servers)
                stats['added'] += added
                this_min += len(servers)
            
            if time.time() - last_log >= 60:
                pstats = proxies.get_stats()
                peak = get_peak_regions()
                regions = [r['name'] for r in peak] if peak else ['Rotating']
                
                log.info(f"[{', '.join(regions)}] {this_min}/min | Added: {stats['added']} | Proxies: {pstats['pool']} | {pstats['success_rate']}%")
                
                if stats['fetch_errors'] > 0:
                    log.warning(f"[FETCH] {stats['fetch_errors']} errors")
                    stats['fetch_errors'] = 0
                
                stats['rate'] = this_min
                this_min = 0
                last_log = time.time()
            
            time.sleep(FETCH_INTERVAL)
            
        except Exception as e:
            log.error(f"Error: {e}")
            time.sleep(1)

# ==================== ENDPOINTS ====================
@app.route('/', methods=['GET'])
@app.route('/status', methods=['GET'])
def status():
    peak = get_peak_regions()
    return jsonify({
        'status': 'running',
        'stats': stats,
        'proxy': proxies.get_stats(),
        'peak_regions': [r['name'] for r in peak] if peak else ['Rotating']
    })

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'proxies': proxies.get_stats()['pool']})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8001))
    
    log.info(f"[STARTUP] Mini API on port {port}")
    log.info(f"[CONFIG] Using SOCKS5 proxies")
    
    # Show status
    for r in REGIONS:
        hour = get_hour(r['utc_offset'])
        status = "🟢 PEAK" if PEAK_START <= hour < PEAK_END else ""
        log.info(f"  {r['name']}: {hour}:00 {status}")
    
    # Start
    peak = get_peak_regions()
    proxies._fetch(random.choice(peak) if peak else random.choice(REGIONS))
    
    threading.Thread(target=run, daemon=True).start()
    app.run(host='0.0.0.0', port=port, threaded=True)
