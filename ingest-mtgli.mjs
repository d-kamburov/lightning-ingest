#!/usr/bin/env node
/**
 * MTG-LI lightning ingest — Meteosat Third Generation Lightning Imager.
 * Companion to ingest-glm.mjs; covers Europe + Africa + Middle East.
 * Writes to the same Cloudflare R2 `lightning-buffer` bucket with the
 * same record schema so /lightning/recent serves both transparently.
 */
import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { File as H5File, ready as h5Ready, FS as H5FS } from 'h5wasm/node';
import JSZip from 'jszip';

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
  const buf = new Uint8Array(await r.arrayBuffer());
  // EUMETSAT wraps each product in a ZIP that bundles the NetCDF
  // payload, a quicklook PNG, and a trailer manifest. Detect the ZIP
  // magic (`PK\x03\x04`) and unpack the first `.nc`-class entry — the
  // archive includes filenames like `...ARC-NC4E_...` for the data
  // and `...ARC-PNG_...` for the preview.
  if (buf[0] === 0x50 && buf[1] === 0x4b) {
    const zip = await JSZip.loadAsync(buf);
    const names = Object.keys(zip.files);
    // MTG-LI LFL products zip together QCK-IMAGE (PNG preview),
    // CHK-TRAIL (statistics + histograms NetCDF), and one or more
    // CHK-BODY entries (the actual per-flash NetCDF chunks). We want
    // BODY; TRAIL has no flash_lat / flash_lon at the root.
    // Return every BODY entry concatenated by the caller; flashes
    // across chunks are merged by the bucket grouper anyway.
    const bodies = names.filter((n) => /CHK-BODY/i.test(n));
    if (bodies.length === 0) {
      // No BODY → product contained only metadata, log the manifest so
      // we can decide whether to keep skipping or follow up.
      console.warn(`[MTI1] ${productId}: no CHK-BODY in ZIP. Entries: ${names.join(',')}`);
      return new Uint8Array();
    }
    // Caller parses one buffer at a time; return an array so the
    // calling code can iterate. Done via a small adapter below.
    return await Promise.all(bodies.map(async (n) =>
      new Uint8Array(await zip.files[n].async('arraybuffer'))
    ));
  }
  return buf;
}

function readDataset(file, names) {
  for (const name of names) {
    try {
      const ds = file.get(name);
      const arr = ds?.to_array?.();
      if (!arr) continue;
      // CF-style scaled storage. MTG-LI stores coords as int16 with
      // scale_factor 0.0027 and add_offset 0; without applying these
      // the parsed values are in raw counts, not degrees. _FillValue
      // marks "no detection" cells we must skip.
      let scale = 1, offset = 0, fill = null;
      try {
        const sf = ds.attrs?.scale_factor;
        const ao = ds.attrs?.add_offset;
        const fv = ds.attrs?.['_FillValue'];
        const v = (x) => x && typeof x === 'object' && '0' in x ? Number(x[0]) : (typeof x === 'number' ? x : (x?.value ?? null));
        const sV = v(sf); if (Number.isFinite(sV) && sV !== 0) scale = sV;
        const oV = v(ao); if (Number.isFinite(oV))             offset = oV;
        const fV = v(fv); if (Number.isFinite(fV))             fill = fV;
      } catch {}
      if (scale === 1 && offset === 0 && fill == null) return arr;
      // Materialise a JS array with scaling + fill replacement so the
      // caller's `Number.isFinite` checks naturally skip masked cells.
      const out = new Array(arr.length);
      for (let i = 0; i < arr.length; i++) {
        const raw = Number(arr[i]);
        if (fill != null && raw === fill) { out[i] = NaN; continue; }
        out[i] = raw * scale + offset;
      }
      return out;
    } catch {}
  }
  return null;
}

/** One-time diagnostic. Dumps the resolved dataset name, its first 3
 *  values, and its HDF5 attributes (scale_factor, units, etc.) so we
 *  can see whether MTG-LI's `flash_lon` is being delivered raw, scaled,
 *  or in non-degree units. Logged once per ingest run. */
let _dumpOnce = false;
function dumpDataset(file, name, label) {
  if (_dumpOnce) return;
  try {
    const ds = file.get(name);
    if (!ds) return;
    const arr = ds.to_array?.();
    const head = Array.isArray(arr) ? arr.slice(0, 3) : (arr?.[0] ?? null);
    const attrs = {};
    try {
      const attrKeys = ds.attrs ? Object.keys(ds.attrs) : [];
      for (const k of attrKeys) attrs[k] = ds.attrs[k]?.value ?? ds.attrs[k];
    } catch {}
    console.log(`[diag] ${label} (${name}) head=${JSON.stringify(head)} attrs=${JSON.stringify(attrs)}`);
  } catch (e) {
    console.log(`[diag] ${label} (${name}) dump failed: ${e.message}`);
  }
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
    // MTG-LI L2 LFL stores lat/lon as int16 with documented
    // scale_factor 0.0027 (degrees per count). The readDataset attribute
    // path doesn't surface the scale value reliably across h5wasm
    // versions, so we apply the documented constant inline and detect
    // the "needs scaling" case by integer magnitude (>90 in lat = raw
    // count). _FillValue -32767 still gets masked here.
    const MTG_LI_SCALE = 0.0027;
    function maybeScale(arr) {
      if (!arr || !arr.length) return arr;
      const v0 = Number(arr[0]);
      if (!Number.isFinite(v0) || Math.abs(v0) <= 90) return arr;
      const out = new Array(arr.length);
      for (let i = 0; i < arr.length; i++) {
        const raw = Number(arr[i]);
        if (raw === -32767) { out[i] = NaN; continue; }
        out[i] = raw * MTG_LI_SCALE;
      }
      return out;
    }
    const latS = maybeScale(lat);
    const lonS = maybeScale(lon);
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
    for (let i = 0; i < latS.length; i++) {
      const la = Number(latS[i]); const lo = Number(lonS[i]);
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
      const out = await downloadProduct(id);
      // downloadProduct now returns either a single Uint8Array (rare,
      // non-ZIP) or an array of Uint8Arrays (one per CHK-BODY chunk).
      const buffers = Array.isArray(out) ? out : [out];
      for (let i = 0; i < buffers.length; i++) {
        if (!buffers[i].length) continue;
        const recs = await parseMtgliFlashes(buffers[i], `${id}#${i}`);
        records.push(...recs);
      }
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
