#!/usr/bin/env python3
"""
MINI API - NAProxy API Integration
Feeds main API with fresh servers using NAProxy residential IPs
"""

import os
import time
import logging
import threading
import requests
import urllib3
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify
import random

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==================== CONFIG ====================
PLACE_ID = 109983668079237
MAIN_API_URL = os.environ.get("MAIN_API_URL", "https://main-api-production-0871.up.railway.app")

# NAProxy API Config
NAPROXY_API_KEY = "b6e6673dfb2404d26759eddd1bdf9d12"
NAPROXY_API_URL = "https://api.naproxy.com/web_v1/ip/get-ip-v3"

# Proxy settings
PROXY_COUNT = 15
PROXY_LIFE = 30
PROXY_PROTOCOL = 1
MIN_PROXIES = 8

# Peak hours
PEAK_START_HOUR = 17
PEAK_END_HOUR = 20

# Fetching
FETCH_THREADS_PEAK = 12
FETCH_THREADS_NORMAL = 6
FETCH_INTERVAL_PEAK = 2
FETCH_INTERVAL_NORMAL = 3
PAGES_PER_CYCLE_PEAK = 40
PAGES_PER_CYCLE_NORMAL = 20
SEND_BATCH_SIZE = 500

logging.basicConfig(
    level=logging.INFO,
    format='[MINI %(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ==================== AUTO TIMEZONE ====================
class TimezoneDetector:
    def __init__(self):
        self.timezone = 'America/New_York'
        self.utc_offset = -5
        self._detect()
    
    def _detect(self):
        try:
            resp = requests.get('http://ip-api.com/json/', timeout=5)
            if resp.ok:
                data = resp.json()
                self.timezone = data.get('timezone', 'America/New_York')
                offsets = {
                    'America/New_York': -5, 'America/Chicago': -6,
                    'America/Denver': -7, 'America/Los_Angeles': -8,
                }
                self.utc_offset = offsets.get(self.timezone, -5)
                log.info(f"[TIMEZONE] {self.timezone}")
        except:
            pass
    
    def get_local_hour(self):
        utc_hour = time.gmtime().tm_hour
        return (utc_hour + self.utc_offset) % 24
    
    def is_peak(self):
        hour = self.get_local_hour()
        return PEAK_START_HOUR <= hour < PEAK_END_HOUR

tz_detector = TimezoneDetector()

def is_peak_hours():
    return tz_detector.is_peak()

def get_config():
    if is_peak_hours():
        return {
            'threads': FETCH_THREADS_PEAK,
            'interval': FETCH_INTERVAL_PEAK,
            'pages': PAGES_PER_CYCLE_PEAK,
            'mode': 'PEAK'
        }
    return {
        'threads': FETCH_THREADS_NORMAL,
        'interval': FETCH_INTERVAL_NORMAL,
        'pages': PAGES_PER_CYCLE_NORMAL,
        'mode': 'NORMAL'
    }

# ==================== NAPROXY POOL ====================
class NAProxyPool:
    def __init__(self):
        self.lock = threading.Lock()
        self.proxies = deque()
        self.stats = {
            'api_calls': 0,
            'ips_fetched': 0,
            'ips_used': 0,
            'success': 0,
            'errors': 0
        }
        
        self.running = True
        threading.Thread(target=self._refresh_loop, daemon=True).start()
    
    def _fetch_proxies(self):
        try:
            params = {
                'app_key': NAPROXY_API_KEY,
                'pt': 9,
                'num': PROXY_COUNT,
                'cc': '',
                'state': '',
                'city': '',
                'life': PROXY_LIFE,
                'protocol': PROXY_PROTOCOL,
                'format': 'txt',
                'lb': '%5Cr%5Cn',
                'sep': ''
            }
            
            resp = requests.get(NAPROXY_API_URL, params=params, timeout=10)
            
            if resp.ok:
                text = resp.text.strip()
                if text and not text.startswith('{'):
                    lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
                    expire_time = time.time() + PROXY_LIFE
                    
                    with self.lock:
                        self.stats['api_calls'] += 1
                        for line in lines:
                            line = line.strip()
                            if line and ':' in line:
                                self.proxies.append((f"http://{line}", expire_time))
                                self.stats['ips_fetched'] += 1
                    
                    log.info(f"[NAPROXY] +{len(lines)} IPs (pool: {len(self.proxies)})")
                    return True
        except Exception as e:
            log.error(f"[NAPROXY] Error: {e}")
        return False
    
    def _refresh_loop(self):
        while self.running:
            try:
                now = time.time()
                with self.lock:
                    # Remove expired
                    while self.proxies and self.proxies[0][1] < now:
                        self.proxies.popleft()
                    pool_size = len(self.proxies)
                
                if pool_size < MIN_PROXIES:
                    self._fetch_proxies()
                
                time.sleep(5)
            except:
                time.sleep(1)
    
    def get_proxy(self):
        now = time.time()
        
        with self.lock:
            # Clean expired
            while self.proxies and self.proxies[0][1] < now:
                self.proxies.popleft()
            
            if self.proxies:
                idx = random.randint(0, len(self.proxies) - 1)
                proxy_url, expire = self.proxies[idx]
                if expire - now > 5:
                    self.stats['ips_used'] += 1
                    return {'http': proxy_url, 'https': proxy_url}, proxy_url
        
        # Need to fetch
        self._fetch_proxies()
        
        with self.lock:
            if self.proxies:
                proxy_url, _ = self.proxies[0]
                self.stats['ips_used'] += 1
                return {'http': proxy_url, 'https': proxy_url}, proxy_url
        
        return None, None
    
    def report_success(self):
        with self.lock:
            self.stats['success'] += 1
    
    def report_error(self, proxy_url=None):
        with self.lock:
            self.stats['errors'] += 1
            if proxy_url:
                self.proxies = deque((p, e) for p, e in self.proxies if p != proxy_url)
    
    def get_stats(self):
        with self.lock:
            total = self.stats['success'] + self.stats['errors']
            return {
                'pool_size': len(self.proxies),
                'api_calls': self.stats['api_calls'],
                'ips_fetched': self.stats['ips_fetched'],
                'success_rate': round(self.stats['success'] / max(1, total) * 100, 1)
            }

proxy_pool = NAProxyPool()

# ==================== STATE ====================
class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.fetched = 0
        self.sent = 0
        self.added = 0
        self.last_rate = 0

stats = Stats()

cursors_asc = deque(maxlen=2000)
cursors_desc = deque(maxlen=2000)
cursor_lock = threading.Lock()

# ==================== FETCHING ====================
def fetch_page(cursor=None, sort_order='Desc'):
    proxy, proxy_url = proxy_pool.get_proxy()
    if not proxy:
        return [], None, sort_order
    
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
            url, params=params, proxies=proxy, timeout=15, verify=False,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
        )
        
        if resp.status_code == 200:
            data = resp.json()
            proxy_pool.report_success()
            return data.get('data', []), data.get('nextPageCursor'), sort_order
        else:
            proxy_pool.report_error(proxy_url)
            return [], None, sort_order
            
    except Exception:
        proxy_pool.report_error(proxy_url)
        return [], None, sort_order

def fetch_cycle():
    config = get_config()
    fetches = [(None, 'Asc'), (None, 'Desc')]
    
    with cursor_lock:
        asc_list = list(cursors_asc)
        desc_list = list(cursors_desc)
        depth = 25 if config['mode'] == 'PEAK' else 15
        
        for c in asc_list[-min(depth, len(asc_list)):]:
            fetches.append((c, 'Asc'))
        for c in desc_list[-min(depth, len(desc_list)):]:
            fetches.append((c, 'Desc'))
    
    fetches = fetches[:config['pages']]
    
    all_servers = []
    seen = set()
    new_asc, new_desc = [], []
    
    with ThreadPoolExecutor(max_workers=config['threads']) as executor:
        futures = {executor.submit(fetch_page, c, s): (c, s) for c, s in fetches}
        
        for future in as_completed(futures):
            try:
                servers, next_cursor, sort_order = future.result()
                
                for s in servers:
                    sid = s.get('id')
                    players = s.get('playing', 0)
                    if players >= 5 and sid and sid not in seen:
                        seen.add(sid)
                        all_servers.append({'id': sid, 'players': players})
                
                if next_cursor:
                    (new_asc if sort_order == 'Asc' else new_desc).append(next_cursor)
            except:
                pass
    
    with cursor_lock:
        cursors_asc.extend(new_asc)
        cursors_desc.extend(new_desc)
    
    return all_servers

def send_to_main(servers):
    if not servers:
        return 0
    
    total = 0
    for i in range(0, len(servers), SEND_BATCH_SIZE):
        batch = servers[i:i + SEND_BATCH_SIZE]
        try:
            resp = requests.post(f"{MAIN_API_URL}/add-pool", json={'servers': batch, 'source': 'mini-api'}, timeout=10)
            if resp.ok:
                total += resp.json().get('added', 0)
        except:
            pass
    return total

# ==================== MAIN LOOP ====================
def run_loop():
    last_log = time.time()
    servers_this_min = 0
    
    while True:
        try:
            config = get_config()
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
                proxy_stats = proxy_pool.get_stats()
                log.info(f"[{config['mode']}] {servers_this_min}/min | Added: {stats.added} | Proxies: {proxy_stats['pool_size']} | Success: {proxy_stats['success_rate']}%")
                servers_this_min = 0
                last_log = time.time()
            
            time.sleep(config['interval'])
        except Exception as e:
            log.error(f"Error: {e}")
            time.sleep(1)

# ==================== ENDPOINTS ====================
@app.route('/', methods=['GET'])
@app.route('/status', methods=['GET'])
def status():
    config = get_config()
    proxy_stats = proxy_pool.get_stats()
    with stats.lock:
        return jsonify({
            'status': 'running',
            'mode': config['mode'],
            'is_peak': is_peak_hours(),
            'local_hour': tz_detector.get_local_hour(),
            'fetched': stats.fetched,
            'sent': stats.sent,
            'added': stats.added,
            'rate_per_min': stats.last_rate,
            'proxy': proxy_stats
        })

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'proxies': proxy_pool.get_stats()['pool_size']})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8001))
    
    log.info(f"[STARTUP] Mini API on port {port}")
    log.info(f"[NAPROXY] Using API with Smart Region")
    
    proxy_pool._fetch_proxies()
    threading.Thread(target=run_loop, daemon=True).start()
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
