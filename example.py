#!/usr/bin/env python3
import base64
import hashlib
import hmac
import os
import time
import urllib.parse

import requests
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key

BASE_URL = "https://api.binance.com"

API_PATH = "/sapi/v1/margin/available-inventory"
REQUEST_TIMEOUT_SEC = 5
INTERVAL_SEC = 20
TYPES = ["MARGIN", "ISOLATED"]
ASSETS = ["BTC", "SOL", "ETH"]

API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")
PRIVATE_KEY_PATH = os.getenv("BINANCE_PRIVATE_KEY_PATH", "ed25519-private.pem")
PRIVATE_KEY_PASSWORD = os.getenv("BINANCE_PRIVATE_KEY_PASSWORD", "")
SIG_ALGO = os.getenv("BINANCE_SIG_ALGO", "ed25519").lower()  # ed25519, rsa, or hmac


def load_private_key(path):
    with open(path, "rb") as f:
        password = PRIVATE_KEY_PASSWORD.encode("utf-8") if PRIVATE_KEY_PASSWORD else None
        return load_pem_private_key(data=f.read(), password=password)


def sign_payload(private_key, payload, algo):
    payload_bytes = payload.encode("ascii")
    if algo == "hmac":
        signature = hmac.new(API_SECRET.encode("utf-8"), payload_bytes, hashlib.sha256)
        return signature.hexdigest()
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
    results = {}
    for margin_type in TYPES:
        try:
            results[margin_type] = fetch_available_inventory(
                BASE_URL, private_key, margin_type
            )
        except Exception as exc:
            print(f"[{now}] base={BASE_URL} type={margin_type} ERROR {exc}")

    if any(margin_type not in results for margin_type in TYPES):
        print(f"[{now}] base={BASE_URL} types={TYPES} compare=skipped")
        return

    assets_by_type = {
        margin_type: results[margin_type].get("assets", {}) or {}
        for margin_type in TYPES
    }
    diffs = []
    for asset in ASSETS:
        left_value = assets_by_type[TYPES[0]].get(asset, "N/A")
        right_value = assets_by_type[TYPES[1]].get(asset, "N/A")
        if left_value != right_value:
            diffs.append((asset, left_value, right_value))

    if diffs:
        for asset, left_value, right_value in diffs:
            print(
                f"[{now}] base={BASE_URL} asset={asset} "
                f"{TYPES[0]}={left_value} {TYPES[1]}={right_value}"
            )
    else:
        print(f"[{now}] base={BASE_URL} types={TYPES} all_assets_equal")


def main():
    if not API_KEY:
        raise SystemExit("BINANCE_API_KEY is required")
    if SIG_ALGO == "hmac":
        if not API_SECRET:
            raise SystemExit("BINANCE_API_SECRET is required for hmac")
        private_key = None
    else:
        private_key = load_private_key(PRIVATE_KEY_PATH)
    run_once(private_key)
    while True:
        time.sleep(INTERVAL_SEC)
        run_once(private_key)


if __name__ == "__main__":
    main()