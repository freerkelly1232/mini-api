#!/usr/bin/env python3
"""
MINI API - Static IPs with Residential Fallback
Feeds main API with fresh servers
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

# Static IPs
STATIC_IPS = [
    {"host": "45.82.245.107", "port": "6006", "user": "proxy-jjvnubn4uo17", "pass": "CeNOkLV1A3ObkRy4"},
    {"host": "163.171.161.247", "port": "6006", "user": "proxy-jjvnubn4uo17", "pass": "CeNOkLV1A3ObkRy4"},
    {"host": "43.153.175.48", "port": "6006", "user": "proxy-jjvnubn4uo17", "pass": "CeNOkLV1A3ObkRy4"},
    {"host": "185.92.209.125", "port": "6006", "user": "proxy-jjvnubn4uo17", "pass": "CeNOkLV1A3ObkRy4"},
]

# Residential fallback
RESIDENTIAL_PROXY = {
    "host": "us.naproxy.net",
    "port": "1000",
    "user": "proxy-e5a1ntzmrlr3",
    "pass": "Ol43jGdsIuPUNacc"
}

# Peak hours
PEAK_START_HOUR = 17
PEAK_END_HOUR = 20

# Fetching
FETCH_THREADS_PEAK = 8
FETCH_THREADS_NORMAL = 3
FETCH_INTERVAL_PEAK = 2
FETCH_INTERVAL_NORMAL = 4
PAGES_PER_CYCLE_PEAK = 30
PAGES_PER_CYCLE_NORMAL = 15
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
        self.timezone = None
        self.utc_offset = 0
        self._detect()
    
    def _detect(self):
        try:
            resp = requests.get('http://ip-api.com/json/', timeout=5)
            if resp.ok:
                data = resp.json()
                self.timezone = data.get('timezone', 'America/New_York')
                self.utc_offset = self._get_offset(self.timezone)
                log.info(f"[TIMEZONE] {self.timezone} (UTC{self.utc_offset:+d})")
                return
        except:
            pass
        self.timezone = 'America/New_York'
        self.utc_offset = -5
    
    def _get_offset(self, tz_name):
        offsets = {
            'America/New_York': -5, 'America/Chicago': -6,
            'America/Denver': -7, 'America/Los_Angeles': -8,
            'Europe/London': 0, 'Europe/Paris': 1,
        }
        return offsets.get(tz_name, -5)
    
    def get_local_hour(self):
        import time
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

# ==================== SMART IP ROTATION ====================
class SmartIPRotator:
    def __init__(self, static_ips, residential):
        self.static_ips = static_ips
        self.residential = residential
        self.static_index = 0
        self.lock = threading.Lock()
        
        self.static_status = {}
        for i in range(len(static_ips)):
            self.static_status[i] = {
                'usage': 0, 'errors': 0, 'cooldown_until': 0, 'consecutive_errors': 0
            }
        
        self.residential_stats = {'usage': 0, 'fallback_count': 0}
    
    def _build_proxy_url(self, ip_config, session_id=None):
        user = ip_config['user']
        if session_id:
            user = f"{user}_session-{session_id}"
        return f"http://{user}:{ip_config['pass']}@{ip_config['host']}:{ip_config['port']}"
    
    def get_proxy(self):
        with self.lock:
            now = time.time()
            
            for _ in range(len(self.static_ips)):
                idx = self.static_index
                self.static_index = (self.static_index + 1) % len(self.static_ips)
                status = self.static_status[idx]
                
                if now < status['cooldown_until']:
                    continue
                
                status['usage'] += 1
                proxy_url = self._build_proxy_url(self.static_ips[idx])
                return {'http': proxy_url, 'https': proxy_url}, 'static', idx
            
            # Fallback to residential
            self.residential_stats['usage'] += 1
            self.residential_stats['fallback_count'] += 1
            session_id = f"m{random.randint(100000, 999999)}"
            proxy_url = self._build_proxy_url(self.residential, session_id)
            return {'http': proxy_url, 'https': proxy_url}, 'residential', -1
    
    def report_success(self, proxy_type, idx):
        with self.lock:
            if proxy_type == 'static' and idx >= 0:
                self.static_status[idx]['consecutive_errors'] = 0
    
    def report_error(self, proxy_type, idx, is_rate_limit=False):
        with self.lock:
            if proxy_type == 'static' and idx >= 0:
                status = self.static_status[idx]
                status['errors'] += 1
                status['consecutive_errors'] += 1
                
                if is_rate_limit or status['consecutive_errors'] >= 2:
                    cooldown = min(30 + (status['consecutive_errors'] * 15), 120)
                    status['cooldown_until'] = time.time() + cooldown
    
    def get_stats(self):
        with self.lock:
            now = time.time()
            active = sum(1 for s in self.static_status.values() if now >= s['cooldown_until'])
            return {
                'active_static': active,
                'residential_fallbacks': self.residential_stats['fallback_count']
            }

ip_rotator = SmartIPRotator(STATIC_IPS, RESIDENTIAL_PROXY)

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

cursors_asc = deque(maxlen=2000)
cursors_desc = deque(maxlen=2000)
cursor_lock = threading.Lock()

# ==================== FETCHING ====================
def fetch_page(cursor=None, sort_order='Desc'):
    max_retries = 2
    proxy, proxy_type, ip_idx = ip_rotator.get_proxy()
    
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
                proxies=proxy,
                timeout=15,
                verify=False,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
            )
            
            if resp.status_code == 200:
                data = resp.json()
                ip_rotator.report_success(proxy_type, ip_idx)
                return data.get('data', []), data.get('nextPageCursor'), sort_order
            elif resp.status_code == 429:
                ip_rotator.report_error(proxy_type, ip_idx, is_rate_limit=True)
                proxy, proxy_type, ip_idx = ip_rotator.get_proxy()
                time.sleep(1)
                continue
            else:
                ip_rotator.report_error(proxy_type, ip_idx)
                return [], None, sort_order
            
        except Exception:
            ip_rotator.report_error(proxy_type, ip_idx)
            if attempt < max_retries - 1:
                proxy, proxy_type, ip_idx = ip_rotator.get_proxy()
                time.sleep(0.5)
                continue
            with stats.lock:
                stats.errors += 1
            return [], None, sort_order
    
    return [], None, sort_order

def fetch_cycle():
    config = get_config()
    fetches = []
    
    fetches.append((None, 'Asc'))
    fetches.append((None, 'Desc'))
    
    with cursor_lock:
        asc_list = list(cursors_asc)
        desc_list = list(cursors_desc)
        
        depth = 20 if config['mode'] == 'PEAK' else 10
        
        if asc_list:
            for c in asc_list[-min(depth, len(asc_list)):]:
                fetches.append((c, 'Asc'))
        
        if desc_list:
            for c in desc_list[-min(depth, len(desc_list)):]:
                fetches.append((c, 'Desc'))
    
    fetches = fetches[:config['pages']]
    
    all_servers = []
    seen = set()
    new_asc = []
    new_desc = []
    
    with ThreadPoolExecutor(max_workers=config['threads']) as executor:
        futures = {executor.submit(fetch_page, c, s): (c, s) for c, s in fetches}
        
        for future in as_completed(futures):
            try:
                servers, next_cursor, sort_order = future.result()
                
                for s in servers:
                    sid = s.get('id')
                    players = s.get('playing', 0)
                    
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
                        
            except Exception:
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
        except Exception:
            pass
    
    return total_added

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
                with cursor_lock:
                    depth = len(cursors_asc) + len(cursors_desc)
                ip_stats = ip_rotator.get_stats()
                log.info(f"[{config['mode']}] {servers_this_min}/min | Added: {stats.added} | Static: {ip_stats['active_static']}/4 | Resi: {ip_stats['residential_fallbacks']}")
                servers_this_min = 0
                last_log = time.time()
            
            time.sleep(config['interval'])
                
        except Exception as e:
            log.error(f"Loop error: {e}")
            time.sleep(1)

# ==================== ENDPOINTS ====================
@app.route('/', methods=['GET'])
@app.route('/status', methods=['GET'])
def status():
    config = get_config()
    ip_stats = ip_rotator.get_stats()
    
    with stats.lock:
        with cursor_lock:
            return jsonify({
                'status': 'running',
                'mode': config['mode'],
                'is_peak': is_peak_hours(),
                'local_hour': tz_detector.get_local_hour(),
                'timezone': tz_detector.timezone,
                'fetched': stats.fetched,
                'sent': stats.sent,
                'added': stats.added,
                'errors': stats.errors,
                'rate_per_min': stats.last_rate,
                'depth_total': len(cursors_asc) + len(cursors_desc),
                'ip_stats': ip_stats
            })

@app.route('/health', methods=['GET'])
def health():
    config = get_config()
    return jsonify({
        'status': 'ok',
        'mode': config['mode'],
        'is_peak': is_peak_hours()
    })

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8001))
    
    log.info(f"[STARTUP] Mini API on port {port}")
    log.info(f"[CONFIG] 4 Static IPs + Residential fallback")
    log.info(f"[TIMEZONE] {tz_detector.timezone} (UTC{tz_detector.utc_offset:+d})")
    
    threading.Thread(target=run_loop, daemon=True).start()
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
