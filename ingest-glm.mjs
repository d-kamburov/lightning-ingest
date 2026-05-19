#!/usr/bin/env node
/**
 * GLM lightning ingest — runs every minute via GitHub Actions in this
 * public repo (private repos would cost ~$700/mo at this cadence).
 *
 * Pulls the latest GOES-16 (East) and GOES-18 (West) GLM L2 LCFA
 * files from NOAA's public S3 bucket, parses the NetCDF-4 payload
 * with h5wasm, and writes slim per-flash records into the Cloudflare
 * R2 `lightning-buffer` bucket. The Worker's `/lightning/recent`
 * endpoint at gfs-pressure-proxy reads back from there.
 */
import { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { File as H5File, ready as h5Ready, FS as H5FS } from 'h5wasm/node';

const ACCOUNT_ID = mustEnv('R2_ACCOUNT_ID');
const R2_BUCKET = 'lightning-buffer';
const RETENTION_MS = 60 * 60 * 1000;
const BUCKET_STEP_MIN = 10;
const MAX_FILES_PER_RUN = Number(process.env.GLM_MAX_FILES_PER_RUN || 12);
const DRY_RUN = process.env.GLM_DRY_RUN === '1';

// NOAA hosts the GLM bucket as a public S3 bucket — we hit the
// virtual-hosted HTTPS endpoint directly via fetch() rather than the
// AWS SDK, since the v3 SDK insists on resolving AWS credentials even
// for anonymous reads and there's no clean way to opt out.
const NOAA_HOSTS = { G16: 'noaa-goes16.s3.amazonaws.com', G18: 'noaa-goes18.s3.amazonaws.com' };

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
function dayOfYearUTC(d) {
  const start = Date.UTC(d.getUTCFullYear(), 0, 0);
  return Math.floor((d.getTime() - start) / 86400_000);
}
function glmPrefix(d) {
  return `GLM-L2-LCFA/${d.getUTCFullYear()}/${pad(dayOfYearUTC(d), 3)}/${pad(d.getUTCHours())}/`;
}
function round3(x) { return Math.round(x * 1000) / 1000; }
function round4(x) { return Number.isFinite(x) ? Math.round(x * 10000) / 10000 : 0; }

async function streamToString(stream) {
  const chunks = [];
  for await (const c of stream) chunks.push(c);
  return Buffer.concat(chunks).toString('utf-8');
}

async function listRecentGlmFiles(satCode) {
  const now = new Date();
  const prev = new Date(now.getTime() - 3600_000);
  const prefixes = [glmPrefix(prev), glmPrefix(now)];
  const host = NOAA_HOSTS[satCode];
  const keys = [];
  for (const prefix of prefixes) {
    try {
      const url = `https://${host}/?list-type=2&prefix=${encodeURIComponent(prefix)}`;
      const r = await fetch(url);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const xml = await r.text();
      // S3 ListObjectsV2 XML — minimal regex parse since we only need
      // <Key> values. The bucket returns up to 1000 keys per call;
      // GLM's per-prefix file count is bounded (180 per hour per sat)
      // so we never need to paginate.
      const re = /<Key>([^<]+)<\/Key>/g;
      let m;
      while ((m = re.exec(xml))) if (m[1].endsWith('.nc')) keys.push(m[1]);
    } catch (e) {
      console.warn(`[${satCode}] list ${prefix} failed:`, e.message);
    }
  }
  keys.sort();
  return keys;
}

async function readCursor(satCode) {
  try {
    const out = await r2Client.send(new GetObjectCommand({
      Bucket: R2_BUCKET, Key: `cursors/${satCode}.txt`,
    }));
    return (await streamToString(out.Body)).trim();
  } catch { return ''; }
}
async function writeCursor(satCode, key) {
  if (DRY_RUN) return;
  await r2Client.send(new PutObjectCommand({
    Bucket: R2_BUCKET, Key: `cursors/${satCode}.txt`,
    Body: key, ContentType: 'text/plain',
  }));
}

async function downloadGlmFile(satCode, key) {
  const url = `https://${NOAA_HOSTS[satCode]}/${key}`;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
  return new Uint8Array(await r.arrayBuffer());
}

async function parseGlmFlashes(buf, satCode, sourceKey) {
  await h5Ready;
  const fname = `/tmp/glm_${process.pid}_${Math.random().toString(36).slice(2)}.nc`;
  H5FS.writeFile(fname, buf);
  let file;
  try {
    file = new H5File(fname, 'r');
    const lat   = file.get('flash_lat')?.to_array?.();
    const lon   = file.get('flash_lon')?.to_array?.();
    const t1    = file.get('flash_time_offset_of_first_event')?.to_array?.();
    const en    = file.get('flash_energy')?.to_array?.();
    const qflag = file.get('flash_quality_flag')?.to_array?.();
    // product_time is a scalar dataset. h5wasm exposes scalars as a
    // single value via `.value`; calling `.to_array()` on a scalar
    // returns undefined or an empty array depending on version, so
    // grab the raw value and coerce defensively.
    const ptimeDs = file.get('product_time');
    const ptimeRaw = ptimeDs?.value;
    const ptimeNum = typeof ptimeRaw === 'number' ? ptimeRaw : Number(ptimeRaw?.[0]);
    if (!lat || !lon || !t1 || !en || !qflag || !Number.isFinite(ptimeNum)) return [];
    const EPOCH_2000 = Date.UTC(2000, 0, 1, 12, 0, 0);
    const baseMs = EPOCH_2000 + ptimeNum * 1000;
    const out = [];
    for (let i = 0; i < lat.length; i++) {
      if (qflag[i] !== 0) continue;
      const la = Number(lat[i]);
      const lo = Number(lon[i]);
      if (!Number.isFinite(la) || !Number.isFinite(lo)) continue;
      if (Math.abs(la) > 80) continue;
      out.push({
        ts: baseMs + Math.round(Number(t1[i]) * 1000),
        lat: round3(la), lon: round3(lo),
        en: round4(Number(en[i])), sat: satCode,
      });
    }
    return out;
  } catch (e) {
    console.warn(`[${satCode}] parse ${sourceKey} failed:`, e.message);
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

async function pruneOldBuckets() {
  if (DRY_RUN) return;
  const cutoff = Date.now() - RETENTION_MS;
  let ContinuationToken; let pruned = 0;
  do {
    const out = await r2Client.send(new ListObjectsV2Command({
      Bucket: R2_BUCKET, Prefix: 'flashes/', ContinuationToken,
    }));
    for (const c of out.Contents ?? []) {
      const m = /flashes\/(\d{4})-(\d{2})-(\d{2})T(\d{2})-(\d{2})\.json$/.exec(c.Key);
      if (!m) continue;
      const t = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5]);
      if (t + BUCKET_STEP_MIN * 60_000 < cutoff) {
        await r2Client.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: c.Key }));
        pruned++;
      }
    }
    ContinuationToken = out.IsTruncated ? out.NextContinuationToken : undefined;
  } while (ContinuationToken);
  if (pruned) console.log(`pruned ${pruned} expired buckets`);
}

async function ingestSatellite(satCode) {
  const cursor = await readCursor(satCode);
  const all = await listRecentGlmFiles(satCode);
  const fresh = all.filter((k) => k > cursor).slice(-MAX_FILES_PER_RUN);
  if (!fresh.length) { console.log(`[${satCode}] nothing new`); return; }
  console.log(`[${satCode}] ingesting ${fresh.length} of ${all.length}`);
  const records = [];
  for (const key of fresh) {
    try {
      const buf = await downloadGlmFile(satCode, key);
      const recs = await parseGlmFlashes(buf, satCode, key);
      records.push(...recs);
    } catch (e) {
      console.warn(`[${satCode}] ${key} failed:`, e.message);
    }
  }
  const grouped = groupByBucket(records);
  for (const [k, rs] of grouped) await mergeBucket(k, rs);
  await writeCursor(satCode, fresh[fresh.length - 1]);
  console.log(`[${satCode}] +${records.length} flashes / ${grouped.size} buckets`);
}

async function main() {
  console.log(`GLM ingest start ${new Date().toISOString()} dry=${DRY_RUN}`);
  const results = await Promise.allSettled([
    ingestSatellite('G16'),
    ingestSatellite('G18'),
  ]);
  for (const r of results) if (r.status === 'rejected') console.error('error:', r.reason);
  await pruneOldBuckets();
  console.log(`GLM ingest done ${new Date().toISOString()}`);
}

main().catch((e) => { console.error('fatal:', e); process.exit(1); });
