const axios = require("axios");

const IPS = [
  "103.90.136.194",
  "103.90.136.195",
  "103.90.136.196",
  "103.90.136.197",
  "103.90.136.198",
  "156.231.137.92",
  "156.231.137.188",
];
const PORT = 9080;

const PATHS = {
  inventory: "/bapi/margin/v1/public/margin/marketStats/available-inventory",
};

const REQUEST_TIMEOUT_MS = 5000;
const INTERVAL_MS = 60 * 1000;

function normalizeToMs(ts) {
  if (!Number.isFinite(ts)) {
    return null;
  }
  // Heuristic: seconds are typically 10 digits, ms are 13 digits.
  if (ts < 1e12) {
    return ts * 1000;
  }
  return ts;
}

function toUtc(ms) {
  if (!Number.isFinite(ms)) {
    return "N/A";
  }
  return new Date(ms).toISOString();
}

async function fetchInventory(baseUrl) {
  const resp = await axios.get(`${baseUrl}${PATHS.inventory}`, {
    timeout: REQUEST_TIMEOUT_MS,
  });
  const updateTime = Number(resp?.data?.data?.updateTime);
  return { updateTime, updateTimeMs: normalizeToMs(updateTime) };
}

async function fetchForIp(ip) {
  const baseUrl = `http://${ip}:${PORT}`;
  try {
    const inventory = await fetchInventory(baseUrl);

    const now = new Date().toISOString();
    console.log(
      `[${now}] ip=${ip} updateTime=${inventory.updateTime} updateTimeUtc=${toUtc(
        inventory.updateTimeMs
      )}`
    );
  } catch (err) {
    const now = new Date().toISOString();
    const msg = err?.message || String(err);
    console.log(`[${now}] ip=${ip} ERROR ${msg}`);
  }
}

async function runOnce() {
  await Promise.all(IPS.map((ip) => fetchForIp(ip)));
}

async function runLoop() {
  await runOnce();
  setInterval(runOnce, INTERVAL_MS);
}

runLoop();
