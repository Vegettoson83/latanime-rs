//! Latanime Stremio Addon — full Rust/WASM port
//! Serves catalog, meta, and stream endpoints for latanime.org
//! No Puppeteer dependency — browser players route via BRIDGE_URL

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use once_cell::sync::Lazy;
use regex::Regex;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use worker::*;

// ── Constants ─────────────────────────────────────────────────────────────────

const BASE_URL:   &str = "https://latanime.org";
const TMDB_BASE:  &str = "https://api.themoviedb.org/3";
const TMDB_IMG:   &str = "https://image.tmdb.org/t/p/w500";
const CHROME_UA:  &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const IPHONE_UA:  &str = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) \
    AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1";

const TTL_CATALOG:  u64 = 10 * 60 * 1000;
const TTL_META:     u64 = 2 * 60 * 60 * 1000;
const TTL_STREAM:   u64 = 30 * 60 * 1000;
const TTL_KV_STREAM: u64 = 2 * 60 * 60; // seconds for KV expiration

// Players that need browser rendering — route to bridge
const BROWSER_PLAYERS: &[&str] = &[
    "filemoon", "voe.sx", "lancewhosedifficult", "voeunblocked",
    "mxdrop", "dsvplay", "doodstream", "uqload", "upstream",
];

// ── In-memory cache ───────────────────────────────────────────────────────────

struct CacheEntry {
    data:       Value,
    expires_at: f64,
}

thread_local! {
    static CACHE: RefCell<HashMap<String, CacheEntry>> = RefCell::new(HashMap::new());
}

fn cache_get(key: &str) -> Option<Value> {
    CACHE.with(|c| {
        let mut map = c.borrow_mut();
        if let Some(e) = map.get(key) {
            let now = Date::now().as_millis() as f64;
            if now < e.expires_at {
                return Some(e.data.clone());
            }
            map.remove(key);
        }
        None
    })
}

fn cache_set(key: &str, data: Value, ttl_ms: u64) {
    let now = Date::now().as_millis() as f64;
    CACHE.with(|c| {
        // Evict if > 500 entries to bound memory
        let mut map = c.borrow_mut();
        if map.len() > 500 {
            let stale: Vec<String> = map
                .iter()
                .filter(|(_, v)| now > v.expires_at)
                .map(|(k, _)| k.clone())
                .collect();
            for k in stale { map.remove(&k); }
        }
        map.insert(key.to_string(), CacheEntry { data, expires_at: now + ttl_ms as f64 });
    });
}

// ── CORS / JSON helpers ───────────────────────────────────────────────────────

fn cors_headers() -> Headers {
    let mut h = Headers::new();
    let _ = h.set("Access-Control-Allow-Origin", "*");
    let _ = h.set("Access-Control-Allow-Headers", "*");
    let _ = h.set("Content-Type", "application/json");
    h
}

fn json_response(data: &Value, status: u16) -> Result<Response> {
    let body = data.to_string();
    let mut resp = Response::ok(body)?.with_status(status);
    *resp.headers_mut() = cors_headers();
    Ok(resp)
}

fn ok_json(data: Value) -> Result<Response> { json_response(&data, 200) }

// ── Lazy Regex patterns ───────────────────────────────────────────────────────

static RE_SLUG:      Lazy<Regex> = Lazy::new(|| Regex::new(r#"href=["'](?:https?://latanime\.org)?/anime/([a-zA-Z0-9][a-zA-Z0-9\-]{1,})["']"#).unwrap());
static RE_PLAYER:    Lazy<Regex> = Lazy::new(|| Regex::new(r#"<a[^>]+data-player="([A-Za-z0-9+/=]+)"[^>]*>([\s\S]*?)</a>"#).unwrap());
static RE_EPISODE:   Lazy<Regex> = Lazy::new(|| Regex::new(r#"href=["'](?:https?://latanime\.org)?/ver/([a-z0-9\-]+-episodio-(\d+(?:\.\d+)?))["']"#).unwrap());
static RE_CSRF:      Lazy<Regex> = Lazy::new(|| Regex::new(r#"(?:name="csrf-token"[^>]+content|content)="([^"]+)"(?:[^>]+)?(?:name="csrf-token")?"#).unwrap());
static RE_HREF:      Lazy<Regex> = Lazy::new(|| Regex::new(r#"href=["']([^"']+)["']"#).unwrap());
static RE_M3U8:      Lazy<Regex> = Lazy::new(|| Regex::new(r#"https://[^"'\s\\]+\.m3u8[^"'\s\\]*"#).unwrap());
static RE_MP4_FILE:  Lazy<Regex> = Lazy::new(|| Regex::new(r#""file"\s*:\s*"(https?://[^"]+\.mp4[^"]*)""#).unwrap());
static RE_MF_CDN:    Lazy<Regex> = Lazy::new(|| Regex::new(r#"https://download\d+\.mediafire\.com[^"'\s]+"#).unwrap());
static RE_PD_ID:     Lazy<Regex> = Lazy::new(|| Regex::new(r#"pixeldrain\.com/(?:u/|l/)([a-zA-Z0-9]+)"#).unwrap());
static RE_STAPE:     Lazy<Regex> = Lazy::new(|| Regex::new(r#"get_video\?[^'"]+['"]([^'"]+)['"]\s*\+\s*['"]([^'"]+)['"]"#).unwrap());
static RE_SF_CODE:   Lazy<Regex> = Lazy::new(|| Regex::new(r#"(?:savefiles\.com|streamhls\.to)/(?:e/)?([a-z0-9]+)"#).unwrap());
static RE_CLAPPR:    Lazy<Regex> = Lazy::new(|| Regex::new(r#"sources:\s*\["([^"]+\.m3u8[^"]*)"\]"#).unwrap());
static RE_OG_IMAGE:  Lazy<Regex> = Lazy::new(|| Regex::new(r#"<meta[^>]+property="og:image"[^>]+content="([^"]+)""#).unwrap());
static RE_TITLE_H2:  Lazy<Regex> = Lazy::new(|| Regex::new(r#"<h2[^>]*>([\s\S]*?)</h2>"#).unwrap());
static RE_TITLE_TAG: Lazy<Regex> = Lazy::new(|| Regex::new(r#"<title>(.*?)\s*[—\-|]"#).unwrap());
static RE_DESC:      Lazy<Regex> = Lazy::new(|| Regex::new(r#"<meta[^>]+name="description"[^>]+content="([^"]+)""#).unwrap());
static RE_GENRE:     Lazy<Regex> = Lazy::new(|| Regex::new(r#"href="[^"]*/genero/[^"]*"[^>]*>([\s\S]*?)</a>"#).unwrap());
static RE_TAGS:      Lazy<Regex> = Lazy::new(|| Regex::new(r#"<[^>]+>"#).unwrap());
static RE_SW_ID:     Lazy<Regex> = Lazy::new(|| Regex::new(r#"/(?:e/|f/)?([a-zA-Z0-9]{8,})/?(?:\?|$)"#).unwrap());
static RE_GF_CODE:   Lazy<Regex> = Lazy::new(|| Regex::new(r#"gofile\.io/(?:d|download)/([a-zA-Z0-9]+)"#).unwrap());

fn strip_tags(s: &str) -> String {
    RE_TAGS.replace_all(s, "").trim().to_string()
}

// ── fetchHtml — proxy chain ───────────────────────────────────────────────────

async fn fetch_html(url: &str, bridge_url: Option<&str>) -> Result<String> {
    let encoded = urlencoding::encode(url);

    // Phase 1: direct + bridge in parallel
    let direct_fut = fetch_direct(url);
    let bridge_html = if let Some(burl) = bridge_url {
        let burl = burl.trim().to_string();
        let bridge_url_full = format!("{}/fetch?url={}", burl, encoded);
        Some(fetch_url_raw(&bridge_url_full, None, 12000))
    } else {
        None
    };

    // Try direct first (fast path)
    match direct_fut.await {
        Ok(html) if html.len() > 500 => return Ok(html),
        _ => {}
    }

    // Try bridge
    if let Some(b) = bridge_html {
        match b.await {
            Ok(html) if html.len() > 500 => {
                console_log!("[fetchHtml] bridge succeeded for {}", url);
                return Ok(html);
            }
            _ => {}
        }
    }

    // Phase 2: free proxies sequentially
    let proxies = [
        format!("https://api.allorigins.win/raw?url={}", encoded),
        format!("https://api.codetabs.com/v1/proxy?quest={}", encoded),
        format!("https://corsproxy.io/?{}", encoded),
    ];

    for proxy_url in &proxies {
        match fetch_url_raw(proxy_url, Some(CHROME_UA), 10000).await {
            Ok(html) if html.len() > 500 => {
                let name = proxy_url.split('/').nth(2).unwrap_or("proxy");
                console_log!("[fetchHtml] {} succeeded for {}", name, url);
                return Ok(html);
            }
            _ => {}
        }
    }

    Err(Error::from(format!("All proxies failed for {}", url)))
}

async fn fetch_direct(url: &str) -> Result<String> {
    let mut headers = Headers::new();
    headers.set("User-Agent", CHROME_UA)?;
    headers.set("Accept", "text/html,application/xhtml+xml,*/*;q=0.8")?;
    headers.set("Accept-Language", "es-MX,es;q=0.9,en-US;q=0.8")?;
    headers.set("Referer", "https://www.google.com/")?;
    headers.set("Cache-Control", "max-age=0")?;

    let req = Request::new_with_init(url, RequestInit::new()
        .with_method(Method::Get)
        .with_headers(headers))?;

    let mut resp = Fetch::Request(req).send().await?;
    if resp.status_code() != 200 {
        return Err(Error::from(format!("HTTP {}", resp.status_code())));
    }
    resp.text().await
}

async fn fetch_url_raw(url: &str, ua: Option<&str>, _timeout_ms: u64) -> Result<String> {
    let mut headers = Headers::new();
    headers.set("User-Agent", ua.unwrap_or(CHROME_UA))?;

    let req = Request::new_with_init(url, RequestInit::new()
        .with_method(Method::Get)
        .with_headers(headers))?;

    let mut resp = Fetch::Request(req).send().await?;
    if !resp.status_code().eq(&200) {
        return Err(Error::from(format!("HTTP {}", resp.status_code())));
    }
    resp.text().await
}

// ── TMDB ──────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct TmdbResult {
    poster:      String,
    background:  String,
    description: String,
    year:        String,
}

async fn fetch_tmdb(name: &str, key: &str) -> Option<TmdbResult> {
    if key.is_empty() { return None; }

    // Clean name: strip language suffixes, normalize season
    let clean = {
        let re_lang = Regex::new(r"(?i)\s+(Latino|Castellano|Japones|Japonés|Sub\s+Español)$").ok()?;
        let re_s    = Regex::new(r"(?i)\s+S(\d+)$").ok()?;
        let tmp = re_lang.replace(name, "");
        re_s.replace(&tmp, " Season $1").trim().to_string()
    };

    let url = format!(
        "{}/search/tv?api_key={}&query={}&language=es-ES",
        TMDB_BASE, key, urlencoding::encode(&clean)
    );

    let req = Request::new_with_init(&url, RequestInit::new()
        .with_method(Method::Get)).ok()?;

    let mut resp = Fetch::Request(req).send().await.ok()?;
    if resp.status_code() != 200 { return None; }

    let data: Value = resp.json().await.ok()?;
    let hit = data["results"].as_array()?.first()?;

    Some(TmdbResult {
        poster: hit["poster_path"].as_str()
            .map(|p| format!("{}{}", TMDB_IMG, p))
            .unwrap_or_default(),
        background: hit["backdrop_path"].as_str()
            .map(|p| format!("https://image.tmdb.org/t/p/w1280{}", p))
            .unwrap_or_default(),
        description: hit["overview"].as_str().unwrap_or("").to_string(),
        year: hit["first_air_date"].as_str()
            .and_then(|d| d.get(..4))
            .unwrap_or("")
            .to_string(),
    })
}

// ── HTML Parsers ──────────────────────────────────────────────────────────────

#[derive(Clone, Serialize)]
struct AnimeCard {
    id:     String,
    name:   String,
    poster: String,
}

fn parse_anime_cards(html: &str) -> Vec<AnimeCard> {
    let mut results = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    for cap in RE_SLUG.captures_iter(html) {
        let slug = cap[1].to_lowercase();
        if !seen.insert(slug.clone()) { continue; }

        // Find a window around the match to extract name & poster
        let start = cap.get(0).map(|m| m.start()).unwrap_or(0);
        let end   = (start + 1200).min(html.len());
        let block = &html[start..end];

        // Name: h3 > h2 > alt > title
        let name = find_between(block, "<h3", "</h3>")
            .or_else(|| find_between(block, "<h2", "</h2>"))
            .map(|s| strip_tags(&s))
            .or_else(|| attr_value(block, "alt"))
            .or_else(|| attr_value(block, "title"))
            .filter(|s| s.len() >= 2)
            .unwrap_or_else(|| slug.replace('-', " "));

        // Poster
        let poster = attr_value_named(block, "data-src")
            .or_else(|| attr_value_named(block, "data-lazy-src"))
            .filter(|s| s.contains(".jpg") || s.contains(".png") || s.contains(".webp") || s.contains(".jpeg"))
            .map(|s| if s.starts_with("http") { s } else { format!("{}{}", BASE_URL, s) })
            .unwrap_or_default();

        results.push(AnimeCard { id: format!("latanime:{}", slug), name, poster });
    }
    results
}

fn find_between(s: &str, open_tag: &str, close_tag: &str) -> Option<String> {
    let start = s.find(open_tag)?;
    let inner_start = s[start..].find('>')? + start + 1;
    let end = s[inner_start..].find(close_tag)? + inner_start;
    Some(s[inner_start..end].to_string())
}

fn attr_value(s: &str, _attr: &str) -> Option<String> {
    // Generic: find alt="..." or title="..."
    let pattern = format!(r#"{}="([^"]+)""#, _attr);
    Regex::new(&pattern).ok()?
        .captures(s)
        .map(|c| c[1].to_string())
}

fn attr_value_named(s: &str, attr: &str) -> Option<String> {
    attr_value(s, attr)
}

fn to_meta_preview(card: &AnimeCard) -> Value {
    json!({
        "id": card.id,
        "type": "series",
        "name": card.name,
        "poster": if card.poster.is_empty() {
            format!("{}/public/img/anime.png", BASE_URL)
        } else {
            card.poster.clone()
        },
        "posterShape": "poster"
    })
}

// ── Catalog / Search ──────────────────────────────────────────────────────────

async fn search_animes(query: &str, bridge_url: Option<&str>) -> Vec<AnimeCard> {
    // Try AJAX first
    if let Ok(home_html) = fetch_html(BASE_URL, bridge_url).await {
        if let Some(csrf) = extract_csrf(&home_html) {
            let mut headers = Headers::new();
            let _ = headers.set("Content-Type", "application/json");
            let _ = headers.set("X-CSRF-TOKEN", &csrf);
            let _ = headers.set("X-Requested-With", "XMLHttpRequest");
            let _ = headers.set("Referer", &format!("{}/", BASE_URL));
            let _ = headers.set("Origin", BASE_URL);
            let _ = headers.set("User-Agent", CHROME_UA);

            let body = json!({ "q": query }).to_string();
            let req = Request::new_with_init(
                &format!("{}/buscar_ajax", BASE_URL),
                RequestInit::new()
                    .with_method(Method::Post)
                    .with_headers(headers)
                    .with_body(Some(body.into())),
            );

            if let Ok(req) = req {
                if let Ok(mut resp) = Fetch::Request(req).send().await {
                    if let Ok(html) = resp.text().await {
                        let results = parse_anime_cards(&html);
                        if !results.is_empty() { return results; }
                    }
                }
            }
        }
    }

    // Fallback: search page
    let url = format!("{}/buscar?q={}", BASE_URL, urlencoding::encode(query));
    if let Ok(html) = fetch_html(&url, bridge_url).await {
        return parse_anime_cards(&html);
    }
    vec![]
}

fn extract_csrf(html: &str) -> Option<String> {
    // <meta name="csrf-token" content="...">
    let re = Regex::new(r#"name="csrf-token"[^>]+content="([^"]+)""#).ok()?;
    re.captures(html).map(|c| c[1].to_string())
        .or_else(|| {
            let re2 = Regex::new(r#"content="([^"]+)"[^>]+name="csrf-token""#).ok()?;
            re2.captures(html).map(|c| c[1].to_string())
        })
}

async fn get_catalog(
    catalog_id: &str,
    extra: &HashMap<String, String>,
    bridge_url: Option<&str>,
    db: Option<&D1Database>,
) -> Result<Value> {
    // Search
    if let Some(q) = extra.get("search").filter(|s| !s.trim().is_empty()) {
        // Try D1 first
        if let Some(db) = db {
            let like = format!("%{}%", q.replace('%', "\\%").replace('_', "\\_"));
            let rows = db.prepare(
                "SELECT id, name, poster FROM anime WHERE name LIKE ?1 ESCAPE '\\' ORDER BY name LIMIT 50"
            ).bind(&[like.into()])?.all().await?;
            let metas: Vec<Value> = rows.results::<DbAnime>()?
                .into_iter()
                .map(|r| to_meta_preview(&AnimeCard { id: r.id, name: r.name, poster: r.poster }))
                .collect();
            if !metas.is_empty() {
                return Ok(json!({ "metas": metas }));
            }
        }
        // Live fallback
        let cards = search_animes(q.trim(), bridge_url).await;
        return Ok(json!({ "metas": cards.iter().map(to_meta_preview).collect::<Vec<_>>() }));
    }

    if catalog_id == "latanime-airing" {
        let html = fetch_html(&format!("{}/emision", BASE_URL), bridge_url).await?;
        let cards = parse_anime_cards(&html);
        return Ok(json!({ "metas": cards.iter().map(to_meta_preview).collect::<Vec<_>>() }));
    }

    if catalog_id == "latanime-directory" {
        let skip: usize = extra.get("skip").and_then(|s| s.parse().ok()).unwrap_or(0);

        // Serve from D1
        if let Some(db) = db {
            let count_row = db.prepare("SELECT COUNT(*) as n FROM anime")
                .first::<serde_json::Value>(None).await?;
            let count = count_row.and_then(|v| v["n"].as_i64()).unwrap_or(0);
            if count > 0 {
                let rows = db.prepare(
                    "SELECT id, name, poster FROM anime ORDER BY name LIMIT 50 OFFSET ?1"
                ).bind(&[(skip as f64).into()])?.all().await?;
                let metas: Vec<Value> = rows.results::<DbAnime>()?
                    .into_iter()
                    .map(|r| to_meta_preview(&AnimeCard { id: r.id, name: r.name, poster: r.poster }))
                    .collect();
                return Ok(json!({ "metas": metas }));
            }
        }

        // Live fallback — two pages
        let page = (skip / 35) + 1;
        let mut all: Vec<AnimeCard> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for p in [page, page + 1] {
            let url = format!("{}/animes?page={}", BASE_URL, p);
            if let Ok(html) = fetch_html(&url, bridge_url).await {
                for card in parse_anime_cards(&html) {
                    if seen.insert(card.id.clone()) { all.push(card); }
                }
            }
        }
        return Ok(json!({ "metas": all.iter().map(to_meta_preview).collect::<Vec<_>>() }));
    }

    // Latest (homepage)
    let html = fetch_html(BASE_URL, bridge_url).await?;
    let cards = parse_anime_cards(&html);
    Ok(json!({ "metas": cards.iter().map(to_meta_preview).collect::<Vec<_>>() }))
}

#[derive(Deserialize)]
struct DbAnime {
    id:     String,
    name:   String,
    poster: String,
}

// ── Meta ──────────────────────────────────────────────────────────────────────

async fn get_meta(id: &str, tmdb_key: &str, bridge_url: Option<&str>) -> Result<Value> {
    let slug = id.replace("latanime:", "");
    let html = fetch_html(&format!("{}/anime/{}", BASE_URL, slug), bridge_url).await?;

    let name = RE_TITLE_H2.captures(&html)
        .map(|c| strip_tags(&c[1]))
        .or_else(|| RE_TITLE_TAG.captures(&html).map(|c| c[1].trim().to_string()))
        .unwrap_or_else(|| slug.clone());

    let poster = RE_OG_IMAGE.captures(&html)
        .map(|c| c[1].to_string())
        .unwrap_or_default();

    let description = RE_DESC.captures(&html)
        .map(|c| c[1].to_string())
        .unwrap_or_default();

    let mut genres: Vec<String> = Vec::new();
    for cap in RE_GENRE.captures_iter(&html) {
        let g = strip_tags(&cap[1]);
        if !g.is_empty() && genres.len() < 10 { genres.push(g); }
    }

    // Episodes
    let mut episodes: Vec<(String, f64)> = Vec::new();
    let mut seen_eps: HashSet<String> = HashSet::new();
    for cap in RE_EPISODE.captures_iter(&html) {
        let ep_path = cap[1].to_string();
        if !seen_eps.insert(ep_path.clone()) { continue; }
        let num: f64 = cap[2].parse().unwrap_or(0.0);
        episodes.push((format!("latanime:{}:{}", slug, num), num));
    }
    episodes.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    let tmdb = fetch_tmdb(&name, tmdb_key).await;

    Ok(json!({
        "meta": {
            "id": id,
            "type": "series",
            "name": name,
            "poster": tmdb.as_ref().map(|t| t.poster.clone()).filter(|s| !s.is_empty()).unwrap_or(poster.clone()),
            "background": tmdb.as_ref().map(|t| t.background.clone()).filter(|s| !s.is_empty()).unwrap_or(poster.clone()),
            "description": tmdb.as_ref().map(|t| t.description.clone()).filter(|s| !s.is_empty()).unwrap_or(description),
            "posterShape": "poster",
            "releaseInfo": tmdb.as_ref().map(|t| t.year.clone()).unwrap_or_default(),
            "genres": genres,
            "videos": episodes.iter().map(|(ep_id, num)| json!({
                "id": ep_id,
                "title": format!("Episodio {}", num),
                "season": 1,
                "episode": num,
                "released": "1970-01-01T00:00:00.000Z"
            })).collect::<Vec<_>>()
        }
    }))
}

// ── Stream Extractors ─────────────────────────────────────────────────────────

async fn extract_mp4upload(url: &str) -> Option<String> {
    let html = fetch_html(url, None).await.ok()?;
    RE_MP4_FILE.captures(&html).map(|c| c[1].to_string())
}

async fn extract_hexload(embed_url: &str) -> Option<String> {
    let file_id = embed_url.split("embed-").nth(1)?
        .split(|c| c == '/' || c == '?').next()?
        .to_string();

    let mut req_headers = Headers::new();
    let _ = req_headers.set("User-Agent", IPHONE_UA);
    let _ = req_headers.set("Referer", "https://latanime.org/");

    let req = Request::new_with_init(embed_url, RequestInit::new()
        .with_method(Method::Get)
        .with_headers(req_headers)).ok()?;

    let embed_resp = Fetch::Request(req).send().await.ok()?;
    let cookies = embed_resp.headers().get("set-cookie").ok()?
        .unwrap_or_default();

    let body = format!("op=download3&id={}&ajax=1&method_free=1", file_id);
    let mut dl_headers = Headers::new();
    let _ = dl_headers.set("User-Agent", IPHONE_UA);
    let _ = dl_headers.set("Referer", embed_url);
    let _ = dl_headers.set("Origin", "https://hexload.com");
    let _ = dl_headers.set("Content-Type", "application/x-www-form-urlencoded");
    let _ = dl_headers.set("X-Requested-With", "XMLHttpRequest");
    let _ = dl_headers.set("Cookie", &cookies);

    let dl_req = Request::new_with_init("https://hexload.com/download", RequestInit::new()
        .with_method(Method::Post)
        .with_headers(dl_headers)
        .with_body(Some(body.into()))).ok()?;

    let mut dl_resp = Fetch::Request(dl_req).send().await.ok()?;
    let data: Value = dl_resp.json().await.ok()?;
    if data["msg"].as_str() == Some("OK") {
        data["result"]["url"].as_str().map(|s| s.to_string())
    } else {
        None
    }
}

async fn extract_savefiles(embed_url: &str) -> Option<String> {
    let file_code = RE_SF_CODE.captures(embed_url)
        .map(|c| c[1].to_string())?;

    let embed_page = format!("https://streamhls.to/e/{}", file_code);
    let body = format!("op=embed&file_code={}&auto=1&referer=https://savefiles.com/{}", file_code, file_code);

    let mut headers = Headers::new();
    let _ = headers.set("User-Agent", IPHONE_UA);
    let _ = headers.set("Referer", &embed_page);
    let _ = headers.set("Origin", "https://streamhls.to");
    let _ = headers.set("Content-Type", "application/x-www-form-urlencoded");
    let _ = headers.set("Accept", "text/html,application/xhtml+xml,*/*;q=0.8");

    let req = Request::new_with_init("https://streamhls.to/dl", RequestInit::new()
        .with_method(Method::Post)
        .with_headers(headers)
        .with_body(Some(body.into()))).ok()?;

    let mut resp = Fetch::Request(req).send().await.ok()?;
    let html = resp.text().await.ok()?;

    RE_CLAPPR.captures(&html)
        .map(|c| c[1].to_string())
        .or_else(|| RE_M3U8.find(&html).map(|m| m.as_str().to_string()))
}

async fn extract_mediafire(url: &str) -> Option<String> {
    let mut headers = Headers::new();
    let _ = headers.set("User-Agent", IPHONE_UA);
    let _ = headers.set("Referer", "https://www.mediafire.com/");
    let _ = headers.set("Accept-Language", "es-MX,es;q=0.9");

    let req = Request::new_with_init(url, RequestInit::new()
        .with_method(Method::Get)
        .with_headers(headers)).ok()?;

    let mut resp = Fetch::Request(req).send().await.ok()?;
    if resp.status_code() != 200 { return None; }
    let html = resp.text().await.ok()?;

    RE_MF_CDN.find(&html).map(|m| m.as_str().to_string())
        .or_else(|| {
            let re = Regex::new(r#"href="(https://download\d+\.mediafire\.com[^"]+)""#).ok()?;
            re.captures(&html).map(|c| c[1].to_string())
        })
}

async fn extract_streamtape(embed_url: &str) -> Option<String> {
    let html = fetch_html(embed_url, None).await.ok()?;
    RE_STAPE.captures(&html)
        .map(|c| format!("https:{}{}", &c[1], &c[2]))
}

async fn extract_streamwish(embed_url: &str) -> Option<String> {
    let file_id = RE_SW_ID.captures(embed_url)
        .map(|c| c[1].to_string())?;

    let base = {
        let url = Url::parse(embed_url).ok()?;
        format!("{}://{}", url.scheme(), url.host_str()?)
    };

    let post_url = format!("{}/api/source/{}", base, file_id);
    let post_body = format!("r=&d={}", urlencoding::encode(&base));

    let mut headers = Headers::new();
    let _ = headers.set("User-Agent", CHROME_UA);
    let _ = headers.set("Referer", embed_url);
    let _ = headers.set("Origin", &base);
    let _ = headers.set("Content-Type", "application/x-www-form-urlencoded");
    let _ = headers.set("X-Requested-With", "XMLHttpRequest");

    let req = Request::new_with_init(&post_url, RequestInit::new()
        .with_method(Method::Post)
        .with_headers(headers)
        .with_body(Some(post_body.into()))).ok()?;

    let mut resp = Fetch::Request(req).send().await.ok()?;
    let data: Value = resp.json().await.ok()?;

    let sources = data["data"].as_array()?;
    sources.iter()
        .find(|s| s["type"].as_str() == Some("hls") || s["file"].as_str().map(|f| f.contains(".m3u8")).unwrap_or(false))
        .or_else(|| sources.first())
        .and_then(|s| s["file"].as_str())
        .map(|s| s.to_string())
}

async fn extract_gofile(url: &str) -> Option<String> {
    let content_id = RE_GF_CODE.captures(url)?.get(1)?.as_str().to_string();

    // Get guest token
    let mut resp = Fetch::Request(
        Request::new_with_init("https://api.gofile.io/accounts", RequestInit::new()
            .with_method(Method::Post)).ok()?
    ).send().await.ok()?;
    let token_data: Value = resp.json().await.ok()?;
    let token = token_data["data"]["token"].as_str()?.to_string();

    // Fetch content
    let api_url = format!("https://api.gofile.io/contents/{}?wt=4fd6sg89d7s6", content_id);
    let mut headers = Headers::new();
    let _ = headers.set("Authorization", &format!("Bearer {}", token));
    let req = Request::new_with_init(&api_url, RequestInit::new()
        .with_method(Method::Get)
        .with_headers(headers)).ok()?;

    let mut resp = Fetch::Request(req).send().await.ok()?;
    let content: Value = resp.json().await.ok()?;

    let children = content["data"]["children"].as_object()?;
    for child in children.values() {
        if child["type"].as_str() == Some("file") {
            if let Some(mime) = child["mimetype"].as_str() {
                if mime.starts_with("video/") {
                    return child["link"].as_str().map(|s| s.to_string());
                }
            }
        }
    }
    None
}

async fn extract_via_bridge(embed_url: &str, bridge_url: &str) -> Option<String> {
    let url = format!("{}/extract?url={}", bridge_url, urlencoding::encode(embed_url));
    let req = Request::new_with_init(&url, RequestInit::new()
        .with_method(Method::Get)).ok()?;
    let mut resp = Fetch::Request(req).send().await.ok()?;
    if resp.status_code() != 200 { return None; }
    let data: Value = resp.json().await.ok()?;
    data["url"].as_str().map(|s| s.to_string())
}

fn needs_bridge(url: &str) -> bool {
    BROWSER_PLAYERS.iter().any(|p| url.contains(p))
}

// ── Streams ───────────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct StreamEntry {
    url:           String,
    title:         String,
    #[serde(rename = "behaviorHints")]
    behavior_hints: Value,
}

async fn get_streams(
    raw_id: &str,
    env: &Env,
    worker_base: &str,
) -> Result<Value> {
    let parts: Vec<&str> = raw_id.replace("latanime:", "").splitn(3, ':').collect();
    if parts.len() < 2 { return Ok(json!({ "streams": [] })); }
    let (slug, ep_num) = (parts[0], parts[1]);

    let bridge_url = env.var("BRIDGE_URL").ok().map(|v| v.to_string());
    let bridge = bridge_url.as_deref().map(|s| s.trim()).filter(|s| !s.is_empty());
    let mfp_base = env.var("MFP_URL").ok().map(|v| v.to_string()).filter(|s| !s.is_empty());
    let mfp_pass = env.var("MFP_PASSWORD").ok().map(|v| v.to_string()).unwrap_or_else(|| "latanime".into());

    let ep_url = format!("{}/ver/{}-episodio-{}", BASE_URL, slug, ep_num);
    let html = fetch_html(&ep_url, bridge).await?;

    // Parse embed players
    let mut embed_urls: Vec<(String, String)> = Vec::new(); // (url, name)
    let mut seen_b64: HashSet<String> = HashSet::new();
    for cap in RE_PLAYER.captures_iter(&html) {
        let b64 = cap[1].to_string();
        let name = strip_tags(&cap[2]);
        let name = if name.is_empty() { "Player".to_string() } else { name };
        if !seen_b64.insert(b64.clone()) { continue; }
        let decoded = match B64.decode(&b64) {
            Ok(b) => match String::from_utf8(b) { Ok(s) => s, Err(_) => continue },
            Err(_) => continue,
        };
        let embed_url = if decoded.starts_with("//") {
            format!("https:{}", decoded)
        } else {
            decoded
        };
        if !embed_url.starts_with("http") { continue; }
        embed_urls.push((embed_url, name));
    }

    // Scrape direct download mirrors
    let mut mirror_mf:  Option<String> = None;
    let mut mirror_sf:  Option<String> = None;
    let mut mirror_pd:  Option<String> = None;
    let mut mirror_gf:  Option<String> = None;

    for cap in RE_HREF.captures_iter(&html) {
        let href = cap[1].trim().to_string();
        if href.contains("mediafire.com") && href.contains("/file/") && mirror_mf.is_none() {
            mirror_mf = Some(href);
        } else if href.contains("savefiles.com") && !href.contains("/d/") && mirror_sf.is_none() {
            mirror_sf = Some(href);
        } else if href.contains("pixeldrain.com") && mirror_pd.is_none() {
            mirror_pd = Some(href);
        } else if href.contains("gofile.io") && mirror_gf.is_none() {
            mirror_gf = Some(href);
        }
    }

    if embed_urls.is_empty() && mirror_mf.is_none() && mirror_sf.is_none()
        && mirror_pd.is_none() && mirror_gf.is_none()
    {
        return Ok(json!({ "streams": [] }));
    }

    let hls_proxy = |m3u8_url: &str, referer: &str| -> String {
        if let Some(ref mfp) = mfp_base {
            format!(
                "{}/proxy/hls/manifest.m3u8?d={}&h_Referer={}&h_Origin={}&api_password={}",
                mfp,
                urlencoding::encode(m3u8_url),
                urlencoding::encode(referer),
                urlencoding::encode(&referer.splitn(3, '/').take(3).collect::<Vec<_>>().join("/")),
                mfp_pass
            )
        } else {
            format!("{}/proxy/m3u8?url={}&ref={}",
                worker_base,
                urlencoding::encode(m3u8_url),
                urlencoding::encode(referer)
            )
        }
    };

    let mut streams: Vec<StreamEntry> = Vec::new();
    let mut extracted: HashSet<String> = HashSet::new();

    // ── Priority 1: MediaFire ─────────────────────────────────────────────
    if let Some(ref mf_url) = mirror_mf {
        if let Some(cdn_url) = extract_mediafire(mf_url).await {
            streams.push(StreamEntry {
                url: cdn_url,
                title: "▶ 🔥 MediaFire MP4 — Latino".into(),
                behavior_hints: json!({ "notWebReady": false }),
            });
        }
    }

    // ── Priority 2: Pixeldrain ────────────────────────────────────────────
    if let Some(ref pd_url) = mirror_pd {
        if let Some(id_cap) = RE_PD_ID.captures(pd_url) {
            let pd_api = format!("https://pixeldrain.com/api/file/{}", &id_cap[1]);
            let in_list = streams.iter().any(|s| s.url == pd_api);
            if !in_list {
                streams.push(StreamEntry {
                    url: pd_api,
                    title: "▶ Pixeldrain — Latino".into(),
                    behavior_hints: json!({ "notWebReady": true }),
                });
            }
        }
    }

    // ── Priority 3: GoFile ────────────────────────────────────────────────
    if let Some(ref gf_url) = mirror_gf {
        if let Some(direct_url) = extract_gofile(gf_url).await {
            streams.push(StreamEntry {
                url: direct_url,
                title: "▶ 📂 GoFile MP4 — Latino".into(),
                behavior_hints: json!({ "notWebReady": false }),
            });
        }
    }

    // ── Savefiles mirror ──────────────────────────────────────────────────
    if let Some(ref sf_url) = mirror_sf {
        if let Some(code) = sf_url.split("savefiles.com/").nth(1)
            .and_then(|s| s.split(|c| c == '/' || c == '?').next())
            .filter(|s| s.len() > 3)
        {
            if let Some(m3u8) = extract_savefiles(&format!("https://savefiles.com/{}", code)).await {
                let proxy_url = hls_proxy(&m3u8, "https://streamhls.to/");
                streams.push(StreamEntry {
                    url: proxy_url,
                    title: "▶ Savefiles 1080p — Latino".into(),
                    behavior_hints: json!({ "notWebReady": true }),
                });
                // 480p variant
                let m3u8_480 = m3u8.replace(",_n,", ",_l,").replace("_n,", "_l,");
                if m3u8_480 != m3u8 {
                    streams.push(StreamEntry {
                        url: hls_proxy(&m3u8_480, "https://streamhls.to/"),
                        title: "▶ Savefiles 480p — Latino".into(),
                        behavior_hints: json!({ "notWebReady": true }),
                    });
                }
            }
        }
    }

    // ── Embed players ─────────────────────────────────────────────────────
    for (embed_url, name) in &embed_urls {
        let result: Option<(String, bool)> = // (url, is_hls)

        if embed_url.contains("pixeldrain.com") {
            RE_PD_ID.captures(embed_url)
                .map(|c| (format!("https://pixeldrain.com/api/file/{}", &c[1]), false))
        } else if embed_url.contains("hexload.com") {
            extract_hexload(embed_url).await.map(|u| (u, false))
        } else if embed_url.contains("mp4upload.com") {
            extract_mp4upload(embed_url).await.map(|u| (u, false))
        } else if embed_url.contains("savefiles.com") || embed_url.contains("streamhls.to") {
            extract_savefiles(embed_url).await.map(|u| {
                let is_hls = u.contains(".m3u8");
                (u, is_hls)
            })
        } else if embed_url.contains("streamtape.com") || embed_url.contains("streamtape.net") {
            extract_streamtape(embed_url).await.map(|u| (u, false))
        } else if embed_url.contains("streamwish.") || embed_url.contains("wishembed.")
            || embed_url.contains("filelions.") || embed_url.contains("kibagames.")
        {
            extract_streamwish(embed_url).await.map(|u| {
                let is_hls = u.contains(".m3u8");
                (u, is_hls)
            })
        } else if needs_bridge(embed_url) {
            // Browser-required players → route to bridge
            if let Some(burl) = bridge {
                extract_via_bridge(embed_url, burl).await.map(|u| {
                    // Check KV cache for this bridge result
                    let is_hls = u.contains(".m3u8");
                    (u, is_hls)
                })
            } else {
                None
            }
        } else if let Some(burl) = bridge {
            extract_via_bridge(embed_url, burl).await.map(|u| {
                let is_hls = u.contains(".m3u8");
                (u, is_hls)
            })
        } else {
            None
        };

        if let Some((stream_url, is_hls)) = result {
            let is_sf = stream_url.contains("savefiles.com")
                || stream_url.contains("streamhls.to")
                || stream_url.contains("s3.savefiles")
                || stream_url.contains("s2.savefiles");

            let final_url = if is_hls && is_sf {
                hls_proxy(&stream_url, "https://streamhls.to/")
            } else if is_hls {
                hls_proxy(&stream_url, "https://latanime.org/")
            } else {
                stream_url.clone()
            };

            if !streams.iter().any(|s| s.url == final_url) {
                streams.push(StreamEntry {
                    url: final_url,
                    title: format!("▶ {} — Latino", name),
                    behavior_hints: json!({ "notWebReady": is_hls }),
                });
            }
            extracted.insert(name.clone());
        }
    }

    // Unextracted embeds — pass direct URL as fallback
    for (embed_url, name) in &embed_urls {
        if !extracted.contains(name) {
            streams.push(StreamEntry {
                url: embed_url.clone(),
                title: format!("🌐 {} — Latino", name),
                behavior_hints: json!({ "notWebReady": true }),
            });
        }
    }

    Ok(json!({ "streams": streams }))
}

// ── HLS Proxy ─────────────────────────────────────────────────────────────────

async fn proxy_m3u8(m3u8_url: &str, referer: &str, worker_base: &str) -> Result<Response> {
    let decoded = urlencoding::decode(m3u8_url)
        .map(|s| s.into_owned())
        .unwrap_or_else(|_| m3u8_url.to_string());

    let base = decoded[..decoded.rfind('/').map(|i| i + 1).unwrap_or(decoded.len())].to_string();

    let origin = referer.splitn(4, '/').take(3).collect::<Vec<_>>().join("/");
    let mut headers = Headers::new();
    let _ = headers.set("Referer", referer);
    let _ = headers.set("Origin", &origin);
    let _ = headers.set("User-Agent", IPHONE_UA);

    let req = Request::new_with_init(&decoded, RequestInit::new()
        .with_method(Method::Get)
        .with_headers(headers))?;

    let mut upstream = Fetch::Request(req).send().await?;
    if upstream.status_code() != 200 {
        return Response::error(format!("Upstream {}", upstream.status_code()), upstream.status_code());
    }

    let m3u8_text = upstream.text().await?;
    let is_master = m3u8_text.contains("#EXT-X-STREAM-INF");
    let enc_ref = urlencoding::encode(referer);

    let rewritten: String = m3u8_text.lines().map(|line| {
        let trimmed = line.trim();
        if trimmed.starts_with('#') || trimmed.is_empty() {
            return line.to_string();
        }
        let abs_url = if trimmed.starts_with("http") {
            trimmed.to_string()
        } else {
            format!("{}{}", base, trimmed)
        };
        if is_master || abs_url.contains(".m3u8") {
            format!("{}/proxy/m3u8?url={}&ref={}",
                worker_base, urlencoding::encode(&abs_url), enc_ref)
        } else {
            format!("{}/proxy/seg?url={}&ref={}",
                worker_base, urlencoding::encode(&abs_url), enc_ref)
        }
    }).collect::<Vec<_>>().join("\n");

    let mut out_headers = Headers::new();
    let _ = out_headers.set("Content-Type", "application/vnd.apple.mpegurl");
    let _ = out_headers.set("Access-Control-Allow-Origin", "*");
    let _ = out_headers.set("Cache-Control", "no-cache");

    Response::ok(rewritten)?.with_headers(out_headers)
}

async fn proxy_seg(seg_url: &str, referer: &str) -> Result<Response> {
    let decoded = urlencoding::decode(seg_url)
        .map(|s| s.into_owned())
        .unwrap_or_else(|_| seg_url.to_string());

    let origin = referer.splitn(4, '/').take(3).collect::<Vec<_>>().join("/");
    let mut headers = Headers::new();
    let _ = headers.set("Referer", referer);
    let _ = headers.set("Origin", &origin);
    let _ = headers.set("User-Agent", IPHONE_UA);

    let req = Request::new_with_init(&decoded, RequestInit::new()
        .with_method(Method::Get)
        .with_headers(headers))?;

    let mut upstream = Fetch::Request(req).send().await?;
    if upstream.status_code() != 200 {
        return Response::error(format!("Upstream {}", upstream.status_code()), upstream.status_code());
    }

    let bytes = upstream.bytes().await?;
    let ct = upstream.headers().get("Content-Type")?.unwrap_or_else(|| "video/MP2T".into());

    let mut out_headers = Headers::new();
    let _ = out_headers.set("Content-Type", &ct);
    let _ = out_headers.set("Access-Control-Allow-Origin", "*");
    let _ = out_headers.set("Cache-Control", "public, max-age=3600");

    Response::from_bytes(bytes)?.with_headers(out_headers)
}

// ── Catalog Sync (cron) ───────────────────────────────────────────────────────

async fn sync_catalog(env: &Env) -> Result<Value> {
    let db = env.d1("CATALOG_DB")?;
    let bridge_url = env.var("BRIDGE_URL").ok().map(|v| v.to_string());
    let bridge = bridge_url.as_deref().map(|s| s.trim()).filter(|s| !s.is_empty());

    let start = Date::now().as_millis() as f64;
    let now_ms = start;
    let mut inserted = 0usize;
    let mut empty_consec = 0u32;
    let mut page = 1u32;

    while page <= 130 {
        let url = format!("{}/animes?page={}", BASE_URL, page);
        page += 1;

        match fetch_html(&url, bridge).await {
            Ok(html) => {
                let cards = parse_anime_cards(&html);
                if cards.is_empty() {
                    empty_consec += 1;
                    if empty_consec >= 10 { break; }
                    continue;
                }
                empty_consec = 0;

                for chunk in cards.chunks(50) {
                    let stmts: Result<Vec<D1PreparedStatement>> = chunk.iter().map(|c| {
                        db.prepare(
                            "INSERT INTO anime (id, slug, name, poster, synced_at) VALUES (?1,?2,?3,?4,?5) \
                             ON CONFLICT(id) DO UPDATE SET name=excluded.name, poster=excluded.poster, synced_at=excluded.synced_at"
                        ).bind(&[
                            c.id.clone().into(),
                            c.id.replace("latanime:", "").into(),
                            c.name.clone().into(),
                            c.poster.clone().into(),
                            now_ms.into(),
                        ])
                    }).collect();
                    db.batch(stmts?).await?;
                    inserted += chunk.len();
                }
            }
            Err(e) => {
                console_error!("[sync] page {} error: {:?}", page - 1, e);
            }
        }
    }

    Ok(json!({
        "inserted": inserted,
        "elapsed_ms": Date::now().as_millis() as f64 - start
    }))
}

// ── Manifest ──────────────────────────────────────────────────────────────────

fn manifest() -> Value {
    json!({
        "id": "com.latanime.stremio",
        "version": "5.0.0",
        "name": "Latanime",
        "description": "Anime Latino y Castellano desde latanime.org — Rust/WASM",
        "logo": "https://latanime.org/public/img/logito.png",
        "resources": ["catalog", "meta", "stream"],
        "types": ["series"],
        "catalogs": [
            { "type": "series", "id": "latanime-latest",    "name": "Latanime — Recientes",  "extra": [{ "name": "search", "isRequired": false }] },
            { "type": "series", "id": "latanime-airing",    "name": "Latanime — En Emisión",  "extra": [] },
            { "type": "series", "id": "latanime-directory", "name": "Latanime — Directorio",  "extra": [{ "name": "search", "isRequired": false }, { "name": "skip", "isRequired": false }] }
        ],
        "idPrefixes": ["latanime:"]
    })
}

// ── Main Handler ──────────────────────────────────────────────────────────────

#[event(fetch)]
pub async fn fetch_handler(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    let url = req.url()?;
    let path = url.path();

    // OPTIONS pre-flight
    if req.method() == Method::Options {
        return Response::ok("")?.with_headers(cors_headers());
    }

    let bridge_url = env.var("BRIDGE_URL").ok().map(|v| v.to_string());
    let bridge = bridge_url.as_deref().map(|s| s.trim()).filter(|s| !s.is_empty());
    let tmdb_key = env.var("TMDB_KEY").ok().unwrap_or_default();
    let tmdb_key = tmdb_key.trim();

    let worker_base = format!("{}://{}", url.scheme(), url.host_str().unwrap_or(""));

    let db = env.d1("CATALOG_DB").ok();

    // ── Manifest ──
    if path == "/" || path == "/manifest.json" {
        return ok_json(manifest());
    }

    // ── Debug ──
    if path == "/debug" {
        return ok_json(json!({
            "bridge": bridge.unwrap_or("not set"),
            "tmdb": if tmdb_key.is_empty() { "not set" } else { "set" },
            "version": "5.0.0-rust"
        }));
    }

    if path == "/admin-sync" {
        let t0 = Date::now().as_millis();
        let result = sync_catalog(&env).await.unwrap_or_else(|e| json!({ "error": e.to_string() }));
        let mut r = result;
        r["total_ms"] = json!(Date::now().as_millis() - t0);
        return ok_json(r);
    }

    if path == "/admin-db" {
        if let Ok(db) = env.d1("CATALOG_DB") {
            let row = db.prepare("SELECT COUNT(*) as count, MAX(synced_at) as last_sync FROM anime")
                .first::<Value>(None).await?;
            return ok_json(row.unwrap_or(json!({ "count": 0 })));
        }
        return ok_json(json!({ "error": "no db" }));
    }

    // ── /catalog/{type}/{id}[/{extra}].json ──
    if let Some(rest) = path.strip_prefix("/catalog/") {
        let rest = rest.trim_end_matches(".json");
        let parts: Vec<&str> = rest.splitn(3, '/').collect();
        if parts.len() >= 2 {
            let catalog_id = parts[1];
            let extra_str  = parts.get(2).copied().unwrap_or("");

            let mut extra: HashMap<String, String> = HashMap::new();
            for pair in extra_str.split('&') {
                if let Some((k, v)) = pair.split_once('=') {
                    extra.insert(k.to_string(), urlencoding::decode(v).map(|s| s.into_owned()).unwrap_or_else(|_| v.to_string()));
                }
            }
            // Also check query string for ?search=
            for (k, v) in url.query_pairs() {
                extra.insert(k.into_owned(), v.into_owned());
            }

            let cache_key = format!("catalog:{}:{}", catalog_id, extra.get("search").or(extra.get("skip")).map(|s| s.as_str()).unwrap_or(""));
            if let Some(cached) = cache_get(&cache_key) {
                return ok_json(cached);
            }

            match get_catalog(catalog_id, &extra, bridge, db.as_ref()).await {
                Ok(result) => {
                    cache_set(&cache_key, result.clone(), TTL_CATALOG);
                    return ok_json(result);
                }
                Err(e) => return ok_json(json!({ "metas": [], "error": e.to_string() })),
            }
        }
    }

    // ── /meta/{type}/{id}.json ──
    if let Some(rest) = path.strip_prefix("/meta/") {
        let rest = rest.trim_end_matches(".json");
        let parts: Vec<&str> = rest.splitn(2, '/').collect();
        if parts.len() >= 2 {
            let id = urlencoding::decode(parts[1]).map(|s| s.into_owned()).unwrap_or_else(|_| parts[1].to_string());
            let cache_key = format!("meta:{}", id);

            if let Some(cached) = cache_get(&cache_key) {
                return ok_json(cached);
            }

            match get_meta(&id, tmdb_key, bridge).await {
                Ok(result) => {
                    cache_set(&cache_key, result.clone(), TTL_META);
                    return ok_json(result);
                }
                Err(e) => return ok_json(json!({ "meta": null, "error": e.to_string() })),
            }
        }
    }

    // ── /stream/{type}/{id}.json ──
    if let Some(rest) = path.strip_prefix("/stream/") {
        let rest = rest.trim_end_matches(".json");
        let parts: Vec<&str> = rest.splitn(2, '/').collect();
        if parts.len() >= 2 {
            let id = urlencoding::decode(parts[1]).map(|s| s.into_owned()).unwrap_or_else(|_| parts[1].to_string());
            let cache_key = format!("stream:{}", id);

            if let Some(cached) = cache_get(&cache_key) {
                return ok_json(cached);
            }

            match get_streams(&id, &env, &worker_base).await {
                Ok(result) => {
                    if result["streams"].as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                        cache_set(&cache_key, result.clone(), TTL_STREAM);
                    }
                    return ok_json(result);
                }
                Err(e) => return ok_json(json!({ "streams": [], "error": e.to_string() })),
            }
        }
    }

    // ── /proxy/m3u8 ──
    if path == "/proxy/m3u8" {
        let params: HashMap<String, String> = url.query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let m3u8_url = params.get("url").map(|s| s.as_str()).unwrap_or("");
        let referer  = params.get("ref").map(|s| s.as_str()).unwrap_or("https://latanime.org/");
        if m3u8_url.is_empty() {
            return Response::error("Missing url", 400);
        }
        return proxy_m3u8(m3u8_url, referer, &worker_base).await;
    }

    // ── /proxy/seg ──
    if path == "/proxy/seg" {
        let params: HashMap<String, String> = url.query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let seg_url = params.get("url").map(|s| s.as_str()).unwrap_or("");
        let referer  = params.get("ref").map(|s| s.as_str()).unwrap_or("https://latanime.org/");
        if seg_url.is_empty() {
            return Response::error("Missing url", 400);
        }
        return proxy_seg(seg_url, referer).await;
    }

    Response::error("Not found", 404)
}

#[event(scheduled)]
pub async fn scheduled(_event: ScheduledEvent, env: Env, ctx: ScheduleContext) {
    ctx.wait_until(async move {
        console_log!("[cron] Starting catalog sync");
        match sync_catalog(&env).await {
            Ok(r)  => console_log!("[cron] Done: {}", r),
            Err(e) => console_error!("[cron] Error: {:?}", e),
        }
    });
}
