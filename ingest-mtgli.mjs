#!/usr/bin/env node
/**
 * MTG-LI lightning ingest — Meteosat Third Generation Lightning Imager.
 * Companion to ingest-glm.mjs; covers Europe + Africa + Middle East.
 * Writes to the same Cloudflare R2 `lightning-buffer` bucket with the
 * same record schema so /lightning/recent serves both transparently.
 */
import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { File as H5File, ready as h5Ready, FS as H5FS } from 'h5wasm/node';

const ACCOUNT_ID = mustEnv('R2_ACCOUNT_ID');
const R2_BUCKET = 'lightning-buffer';
const BUCKET_STEP_MIN = 10;
const MAX_PRODUCTS_PER_RUN = Number(process.env.MTGLI_MAX_PRODUCTS_PER_RUN || 20);
const DRY_RUN = process.env.MTGLI_DRY_RUN === '1';

// Found in the EUMETSAT data store catalog as
// "LI Lightning Flashes - MTG - 0 degree". The older symbolic ID
// EO:EUM:DAT:MTG:LI-L2-LFL returns 404; numeric IDs are the canonical
// reference for MTG collections.
const COLLECTION = 'EO:EUM:DAT:0691';
const COLLECTION_ENC = encodeURIComponent(COLLECTION);
const TOKEN_URL = 'https://api.eumetsat.int/token';
const SEARCH_BASE = 'https://api.eumetsat.int/data/search-products/os';
const DOWNLOAD_URL = (id) =>
  `https://api.eumetsat.int/data/download/collections/${COLLECTION_ENC}/products/${encodeURIComponent(id)}`;

const r2Client = new S3Client({
  region: 'auto',
  endpoint: `https://${ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId:     mustEnv('R2_ACCESS_KEY_ID'),
    secretAccessKey: mustEnv('R2_SECRET_ACCESS_KEY'),
  },
});

function mustEnv(name) {
  const v = process.env[name];
  if (!v) { console.error(`Missing env var: ${name}`); process.exit(1); }
  return v;
}
function pad(n, w = 2) { return String(n).padStart(w, '0'); }
function bucketKey(epochMs) {
  const d = new Date(epochMs);
  const mm = pad(Math.floor(d.getUTCMinutes() / BUCKET_STEP_MIN) * BUCKET_STEP_MIN);
  return `flashes/${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}-${mm}.json`;
}
function round3(x) { return Math.round(x * 1000) / 1000; }
function round4(x) { return Number.isFinite(x) ? Math.round(x * 10000) / 10000 : 0; }

async function streamToString(stream) {
  const chunks = [];
  for await (const c of stream) chunks.push(c);
  return Buffer.concat(chunks).toString('utf-8');
}

let cachedToken = null; let tokenExpiresAt = 0;
async function getAccessToken() {
  const now = Date.now();
  if (cachedToken && now < tokenExpiresAt - 60_000) return cachedToken;
  const auth = Buffer.from(`${mustEnv('EUMETSAT_CONSUMER_KEY')}:${mustEnv('EUMETSAT_CONSUMER_SECRET')}`).toString('base64');
  const r = await fetch(TOKEN_URL, {
    method: 'POST',
    headers: { 'Authorization': `Basic ${auth}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: 'grant_type=client_credentials',
  });
  if (!r.ok) throw new Error(`token HTTP ${r.status}: ${await r.text()}`);
  const body = await r.json();
  cachedToken = body.access_token;
  tokenExpiresAt = now + (Number(body.expires_in) || 3600) * 1000;
  return cachedToken;
}

async function readCursor() {
  try {
    const out = await r2Client.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: 'cursors/MTI1.txt' }));
    return (await streamToString(out.Body)).trim();
  } catch { return ''; }
}
async function writeCursor(productId) {
  if (DRY_RUN) return;
  await r2Client.send(new PutObjectCommand({
    Bucket: R2_BUCKET, Key: 'cursors/MTI1.txt',
    Body: productId, ContentType: 'text/plain',
  }));
}

async function listRecentProducts() {
  const end = new Date();
  const start = new Date(end.getTime() - 30 * 60_000);
  const params = new URLSearchParams({
    format: 'json', pi: COLLECTION,
    dtstart: start.toISOString(), dtend: end.toISOString(),
    si: '0', c: '50', sort: 'start,time,0',
  });
  const r = await fetch(`${SEARCH_BASE}?${params}`);
  if (!r.ok) throw new Error(`search HTTP ${r.status}: ${await r.text()}`);
  const body = await r.json();
  return (body?.features ?? [])
    .map((f) => f?.properties?.identifier)
    .filter((id) => typeof id === 'string')
    .sort();
}

async function downloadProduct(productId) {
  const token = await getAccessToken();
  const r = await fetch(DOWNLOAD_URL(productId), {
    headers: { 'Authorization': `Bearer ${token}` }, redirect: 'follow',
  });
  if (!r.ok) throw new Error(`download ${productId}: HTTP ${r.status}`);
  return new Uint8Array(await r.arrayBuffer());
}

function readDataset(file, names) {
  for (const name of names) {
    try {
      const ds = file.get(name);
      const arr = ds?.to_array?.();
      if (arr) return arr;
    } catch {}
  }
  return null;
}

async function parseMtgliFlashes(buf, sourceKey) {
  await h5Ready;
  const fname = `/tmp/mtgli_${process.pid}_${Math.random().toString(36).slice(2)}.nc`;
  H5FS.writeFile(fname, buf);
  let file;
  try {
    file = new H5File(fname, 'r');
    const lat = readDataset(file, ['flash_lat', 'flash_latitude', 'latitude']);
    const lon = readDataset(file, ['flash_lon', 'flash_longitude', 'longitude']);
    const t   = readDataset(file, ['flash_time', 'time']);
    const en  = readDataset(file, ['flash_radiance', 'flash_energy', 'radiance']);
    if (!lat || !lon || !t) {
      // Dump the file's top-level dataset names once so we can fix the
      // var lookup on the next deploy without another guessing round.
      try {
        const keys = file.keys?.() ?? [];
        console.warn(`[MTI1] ${sourceKey}: required vars not found. Top-level keys: ${JSON.stringify(keys)}`);
      } catch {
        console.warn(`[MTI1] ${sourceKey}: required vars not found, key dump failed`);
      }
      return [];
    }
    const EPOCH_2000 = Date.UTC(2000, 0, 1, 0, 0, 0);
    const out = [];
    for (let i = 0; i < lat.length; i++) {
      const la = Number(lat[i]); const lo = Number(lon[i]);
      if (!Number.isFinite(la) || !Number.isFinite(lo)) continue;
      if (Math.abs(la) > 70) continue;
      out.push({
        ts: EPOCH_2000 + Math.round(Number(t[i]) * 1000),
        lat: round3(la), lon: round3(lo),
        en: round4(en ? Number(en[i]) : 0), sat: 'MTI1',
      });
    }
    return out;
  } catch (e) {
    console.warn(`[MTI1] parse ${sourceKey} failed:`, e.message);
    return [];
  } finally {
    try { file?.close(); } catch {}
    try { H5FS.unlink(fname); } catch {}
  }
}

function groupByBucket(records) {
  const m = new Map();
  for (const r of records) {
    const k = bucketKey(r.ts);
    let arr = m.get(k); if (!arr) { arr = []; m.set(k, arr); }
    arr.push(r);
  }
  return m;
}

async function mergeBucket(key, fresh) {
  let existing = [];
  try {
    const out = await r2Client.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: key }));
    existing = JSON.parse(await streamToString(out.Body));
    if (!Array.isArray(existing)) existing = [];
  } catch {}
  const seen = new Set(existing.map((r) => `${r.ts}|${r.lat}|${r.lon}|${r.sat}`));
  for (const r of fresh) {
    const k = `${r.ts}|${r.lat}|${r.lon}|${r.sat}`;
    if (seen.has(k)) continue;
    seen.add(k); existing.push(r);
  }
  existing.sort((a, b) => a.ts - b.ts);
  if (DRY_RUN) { console.log(`[dry] would write ${existing.length} to ${key}`); return; }
  await r2Client.send(new PutObjectCommand({
    Bucket: R2_BUCKET, Key: key,
    Body: JSON.stringify(existing), ContentType: 'application/json',
  }));
}

async function main() {
  console.log(`MTG-LI ingest start ${new Date().toISOString()} dry=${DRY_RUN}`);
  const cursor = await readCursor();
  const products = await listRecentProducts();
  const fresh = products.filter((p) => p > cursor).slice(0, MAX_PRODUCTS_PER_RUN);
  if (!fresh.length) { console.log(`[MTI1] nothing new`); return; }
  console.log(`[MTI1] ingesting ${fresh.length} of ${products.length}`);
  const records = [];
  for (const id of fresh) {
    try {
      const buf = await downloadProduct(id);
      const recs = await parseMtgliFlashes(buf, id);
      records.push(...recs);
    } catch (e) {
      console.warn(`[MTI1] ${id} failed:`, e.message);
    }
  }
  const grouped = groupByBucket(records);
  for (const [k, rs] of grouped) await mergeBucket(k, rs);
  await writeCursor(fresh[fresh.length - 1]);
  console.log(`[MTI1] +${records.length} flashes / ${grouped.size} buckets`);
}

main().catch((e) => { console.error('fatal:', e); process.exit(1); });
