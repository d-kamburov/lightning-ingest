# lightning-ingest

Pulls real-time lightning detections from NOAA GOES-16/18 (GLM) and EUMETSAT MTG-LI (MTI1), parses the NetCDF-4 payloads, and writes slim per-flash records to a Cloudflare R2 bucket. A separate Worker at `gfs-pressure-proxy` exposes `/lightning/recent` for downstream consumers.

This repo is intentionally **public** so the every-minute GitHub Actions cron runs on the free unlimited tier for public repos. The code here parses public NOAA / EUMETSAT data — nothing proprietary lives in it.

## Setup

1. Create a public GitHub repo and push this directory.
2. Add the following repo secrets under Settings → Secrets and variables → Actions:
   - `R2_ACCOUNT_ID`
   - `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` (R2 API token with read+write on `lightning-buffer`)
   - `EUMETSAT_CONSUMER_KEY` / `EUMETSAT_CONSUMER_SECRET` (from https://api.eumetsat.int/api-key)
3. Run the workflow manually with `dry_run = true` once to verify parsing, then let the cron take over.
