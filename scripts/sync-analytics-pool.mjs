#!/usr/bin/env node
/**
 * Strip evicted wallets from analytics.json.gz so the dashboard's pool
 * count reflects the post-eviction reality.
 *
 * Why this exists: the cron workflow order is
 *   1. Run scanner       → writes analytics.json.gz with leaderboard
 *                          of N active wallets
 *   2. Apply eviction    → mutates wallets.json.gz (status='removed')
 *   3. Commit results
 *
 * Step 2 doesn't touch analytics.json.gz, so the dashboard reads the
 * pre-eviction leaderboard and shows N rather than (N - evicted). This
 * runs as a 4th step right before commit, syncing the leaderboard to
 * match the wallets.json.gz reality.
 *
 * Idempotent. Touches only the leaderboard / summary projection — does
 * not regenerate analytics from scratch.
 *
 * Usage:  node scripts/sync-analytics-pool.mjs
 */
import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const WALLETS_PATH = path.join(ROOT, 'data/wallets.json.gz');
const ANALYTICS_PATH = path.join(ROOT, 'data/analytics.json.gz');

const walletsData = JSON.parse(zlib.gunzipSync(fs.readFileSync(WALLETS_PATH)).toString());
const analytics = JSON.parse(zlib.gunzipSync(fs.readFileSync(ANALYTICS_PATH)).toString());
const pool = walletsData.pool || walletsData;

const removedAddrs = new Set(
  Object.entries(pool)
    .filter(([, w]) => w?.status === 'removed')
    .map(([addr]) => addr.toLowerCase())
);

const beforeCount = (analytics.leaderboard || []).length;
analytics.leaderboard = (analytics.leaderboard || []).filter(
  entry => !removedAddrs.has((entry.address || '').toLowerCase())
);
const afterCount = analytics.leaderboard.length;

// Refresh the rank field on remaining entries — keeps the dashboard's
// rank column consistent with the visible row order.
analytics.leaderboard.forEach((entry, idx) => { entry.rank = idx + 1; });

// Update summary.totalWallets if present so other consumers stay aligned.
if (analytics.summary && typeof analytics.summary.totalWallets === 'number') {
  analytics.summary.totalWallets = afterCount;
}

if (afterCount === beforeCount) {
  console.log(`No-op: leaderboard already at ${afterCount} (no evicted wallets to strip)`);
  process.exit(0);
}

fs.writeFileSync(ANALYTICS_PATH, zlib.gzipSync(Buffer.from(JSON.stringify(analytics))));
console.log(`✓ Synced analytics: leaderboard ${beforeCount} → ${afterCount} (stripped ${beforeCount - afterCount} evicted wallets)`);
