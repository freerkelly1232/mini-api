#!/usr/bin/env python3
"""
MINI API - UK/Brazil/US Peak Hour Rotation
Feeds main API with servers from peak regions
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
NAPROXY_API_URL = "https://api.naproxy.com/web_v1/ip/get-ip-v3"
NAPROXY_API_KEY = "b6e6673dfb2404d26759eddd1bdf9d12"

# Proxy settings
PROXY_COUNT = 15
PROXY_LIFE = 30
MIN_PROXIES = 8

# Peak hours
PEAK_START = 17
PEAK_END = 20

# Regions
REGIONS = [
    {"name": "UK", "cc": "GB", "utc_offset": 0},
    {"name": "Brazil", "cc": "BR", "utc_offset": -3},
    {"name": "US-East", "cc": "US", "state": "NY", "utc_offset": -5},
    {"name": "US-Central", "cc": "US", "state": "TX", "utc_offset": -6},
    {"name": "US-West", "cc": "US", "state": "CA", "utc_offset": -8},
]

# Fetching
FETCH_THREADS = 10
FETCH_INTERVAL = 2
PAGES_PER_CYCLE = 35
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
        
        threading.Thread(target=self._loop, daemon=True).start()
    
    def _fetch(self, region=None):
        try:
            params = {
                'app_key': NAPROXY_API_KEY,
                'pt': 9,
                'num': PROXY_COUNT,
                'life': PROXY_LIFE,
                'protocol': 1,
                'format': 'txt',
                'lb': '%5Cr%5Cn',
                'cc': region.get('cc', '') if region else '',
                'state': region.get('state', '') if region else '',
                'city': '',
                'ep': ''
            }
            
            name = region['name'] if region else 'Any'
            resp = requests.get(NAPROXY_API_URL, params=params, timeout=10)
            
            if resp.ok:
                text = resp.text.strip()
                if text and not text.startswith('{'):
                    lines = text.replace('\r\n', '\n').split('\n')
                    expire = time.time() + PROXY_LIFE
                    
                    with self.lock:
                        for line in lines:
                            line = line.strip()
                            if line and ':' in line:
                                self.proxies.append((f"http://{line}", expire))
                                self.stats['fetched'] += 1
                        self.region = name
                    
                    log.info(f"[PROXY] +{len(lines)} from {name}")
        except Exception as e:
            log.error(f"[PROXY] {e}")
    
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
                'success_rate': round(self.stats['success'] / max(1, total) * 100, 1)
            }

proxies = ProxyPool()

# ==================== STATS ====================
stats = {'fetched': 0, 'sent': 0, 'added': 0, 'rate': 0}
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
    except:
        proxies.error(url)
    
    return [], None

def fetch_cycle():
    fetches = [(None, 'Asc'), (None, 'Desc')]
    
    with cursor_lock:
        for c in list(cursors_asc)[-20:]:
            fetches.append((c, 'Asc'))
        for c in list(cursors_desc)[-20:]:
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
        'peak_regions': [r['name'] for r in peak] if peak else ['Rotating'],
        'all_regions': [
            {'name': r['name'], 'hour': get_hour(r['utc_offset']), 'is_peak': PEAK_START <= get_hour(r['utc_offset']) < PEAK_END}
            for r in REGIONS
        ]
    })

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'proxies': proxies.get_stats()['pool']})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8001))
    
    log.info(f"[STARTUP] Mini API on port {port}")
    log.info(f"[CONFIG] Regions: UK, Brazil, US")
    
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
