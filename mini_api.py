#!/usr/bin/env python3
import os
import requests
import threading
import time
import logging
from flask import Flask, jsonify

# ----------------------
# Configuración básica
# ----------------------
logging.basicConfig(level=logging.INFO, format='[MINI] %(message)s')
app = Flask(__name__)

GAME_ID = os.environ.get("GAME_ID", "109983668079237")
BASE_URL = f"https://games.roblox.com/v1/games/{GAME_ID}/servers/Public?sortOrder=Asc&limit=100"
MAIN_API_URL = "https://main-api-production-79f4.up.railway.app/add-pool"

# ⚡ valores rápidos
SEND_INTERVAL = int(os.environ.get("SEND_INTERVAL", "10"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "5"))

# ----------------------
# Tus proxys
# ----------------------
PROXIES = [
    "http://proxy-e5a1ntzmrlr3_area-VN:Ol43jGdsIuPUNacc@as.naproxy.net:1000"
]

# ----------------------
# Función para obtener todos los servidores
# ----------------------
def fetch_all_roblox_servers(retries=3):
    all_servers = []
    cursor = None
    page_count = 0
    proxy_index = 0

    while True:
        proxy = PROXIES[proxy_index % len(PROXIES)]
        proxies = {"http": proxy, "https": proxy}

        try:
            url = BASE_URL + (f"&cursor={cursor}" if cursor else "")
            page_count += 1
            logging.info(f"[FETCH] Page {page_count} via proxy {proxy} ...")

            r = requests.get(url, proxies=proxies, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                logging.warning("[429] Too Many Requests — cambiando de proxy...")
                proxy_index += 1
                time.sleep(1)
                continue

            r.raise_for_status()
            data = r.json()

            servers = data.get("data", [])
            all_servers.extend(servers)
            cursor = data.get("nextPageCursor")

            logging.info(f"[PAGE {page_count}] +{len(servers)} servers (Total: {len(all_servers)})")

            if not cursor:
                logging.info("[INFO] No hay más páginas disponibles.")
                break

            proxy_index += 1
            time.sleep(0.2)  # ⚡ más rápido

        except requests.exceptions.RequestException as e:
            logging.warning(f"[ERROR] Proxy {proxy} falló: {e}")
            proxy_index += 1
            time.sleep(0.4)  # ⚡ pausa rápida en errores
            if proxy_index >= len(PROXIES) * retries:
                break

    return all_servers

# ----------------------
# Loop principal
# ----------------------
def fetch_and_send():
    while True:
        servers = fetch_all_roblox_servers()

        if not servers:
            logging.warning("No se encontraron servidores en este ciclo.")
            time.sleep(SEND_INTERVAL)
            continue

        job_ids = [s["id"] for s in servers if "id" in s]
        payload = {"servers": job_ids}

        try:
            resp = requests.post(MAIN_API_URL, json=payload, timeout=REQUEST_TIMEOUT)
            if resp.ok:
                added = resp.json().get("added", None)
                logging.info(f"✅ Enviados {len(job_ids)} servers, añadidos: {added}")
            else:
                logging.warning(f"⚠️ MAIN devolvió {resp.status_code}: {resp.text}")
        except Exception as e:
            logging.exception(f"❌ Error al enviar a MAIN: {e}")

        time.sleep(SEND_INTERVAL)

# ----------------------
# Hilo en background
# ----------------------
threading.Thread(target=fetch_and_send, daemon=True).start()

# ----------------------
# Endpoint simple
# ----------------------
@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "mini API running"})

# ----------------------
# Main
# ----------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8001))
    logging.info(f"Mini API local corriendo en puerto {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
