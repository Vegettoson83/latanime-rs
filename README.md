# latanime-rs

Full Rust/WASM port of the Latanime Stremio addon.

## v5.0.0 — Why Rust

The TypeScript worker had persistent Error 1101 crashes caused by JS runtime
non-determinism — promise chain edge cases, GC pauses, and Puppeteer
initialization failures under load. Rust compiles to WASM and runs
deterministically with no JS runtime involvement.

## What changed

- Everything ported to Rust (workers-rs 0.7.5)
- Puppeteer removed — browser-required players route to BRIDGE_URL (Playwright)
- D1 catalog with daily cron sync (4 AM UTC)
- In-memory TTL cache via thread_local RefCell
- Proxy load balancer: direct → bridge → allorigins → codetabs → corsproxy

## Endpoints

| Path | Description |
|------|-------------|
| `/manifest.json` | Stremio manifest |
| `/catalog/series/{id}.json` | Catalog (latest/airing/directory) |
| `/meta/series/{id}.json` | Anime metadata + episodes |
| `/stream/series/{id}.json` | Stream URLs |
| `/proxy/m3u8` | HLS manifest proxy |
| `/proxy/seg` | HLS segment proxy |
| `/admin-sync` | Trigger D1 catalog sync |
| `/admin-db` | D1 row count + last sync |
| `/debug` | Config status |

## Build

```bash
rustup target add wasm32-unknown-unknown
cargo install worker-build
worker-build --release
npx wrangler deploy
```
