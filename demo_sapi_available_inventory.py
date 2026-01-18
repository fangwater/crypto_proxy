#!/usr/bin/env python3
import base64
import os
import time
import urllib.parse

import requests
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key

BASE_URLS = [
    "https://api.binance.com",
    "https://api-gcp.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]

API_PATH = "/sapi/v1/margin/available-inventory"
REQUEST_TIMEOUT_SEC = 5
INTERVAL_SEC = 60
TYPES = ["MARGIN", "ISOLATED"]

API_KEY = os.getenv("BINANCE_API_KEY", "")
PRIVATE_KEY_PATH = os.getenv("BINANCE_PRIVATE_KEY_PATH", "ed25519-private.pem")
SIG_ALGO = os.getenv("BINANCE_SIG_ALGO", "ed25519").lower()  # ed25519 or rsa


def load_private_key(path):
    with open(path, "rb") as f:
        return load_pem_private_key(data=f.read(), password=None)


def sign_payload(private_key, payload, algo):
    payload_bytes = payload.encode("ascii")
    if algo == "rsa":
        signature = private_key.sign(
            payload_bytes,
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
    else:
        # ed25519: sign(payload) directly
        signature = private_key.sign(payload_bytes)
    return base64.b64encode(signature).decode("ascii")


def to_utc_ms(ts):
    if ts is None:
        return "N/A"
    # heuristic: seconds vs milliseconds
    if ts < 1e12:
        ts *= 1000
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts / 1000.0)) + "Z"


def fetch_available_inventory(base_url, private_key, margin_type):
    params = {
        "type": margin_type,
        "timestamp": int(time.time() * 1000),
        "recvWindow": 5000,
    }
    payload = urllib.parse.urlencode(params, encoding="UTF-8")
    signature = sign_payload(private_key, payload, SIG_ALGO)
    params["signature"] = signature

    headers = {"X-MBX-APIKEY": API_KEY}
    resp = requests.get(
        f"{base_url}{API_PATH}",
        headers=headers,
        params=params,
        timeout=REQUEST_TIMEOUT_SEC,
    )
    return resp.json()


def run_once(private_key):
    now = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"
    for base_url in BASE_URLS:
        for margin_type in TYPES:
            try:
                data = fetch_available_inventory(base_url, private_key, margin_type)
                update_time = data.get("updateTime")
                print(
                    f"[{now}] base={base_url} type={margin_type} "
                    f"updateTime={update_time} updateTimeUtc={to_utc_ms(update_time)}"
                )
            except Exception as exc:
                print(f"[{now}] base={base_url} type={margin_type} ERROR {exc}")


def main():
    if not API_KEY:
        raise SystemExit("BINANCE_API_KEY is required")
    private_key = load_private_key(PRIVATE_KEY_PATH)
    run_once(private_key)
    while True:
        time.sleep(INTERVAL_SEC)
        run_once(private_key)


if __name__ == "__main__":
    main()
