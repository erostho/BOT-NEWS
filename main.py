import os
import time
import re
import json
import hashlib
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone
import urllib.parse
import feedparser
import requests
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from dateutil import parser
try:
    import tweepy
except Exception:
    tweepy = None

try:
    import praw
except Exception:
    praw = None


load_dotenv()

# =====================
# ENV
# =====================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")

TWITTER_BEARER = os.getenv("TWITTER_BEARER", "")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_SECRET = os.getenv("REDDIT_SECRET", "")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "geo-news-worker/1.0")

ENABLE_X = os.getenv("ENABLE_X", "true").lower() == "true"
ENABLE_NEWSAPI = os.getenv("ENABLE_NEWSAPI", "true").lower() == "true"
ENABLE_REDDIT = os.getenv("ENABLE_REDDIT", "true").lower() == "true"
ENABLE_GDELT = os.getenv("ENABLE_GDELT", "true").lower() == "true"
ENABLE_TG_SOURCE = os.getenv("ENABLE_TG_SOURCE", "false").lower() == "true"
MAIN_LOOP_SLEEP = int(os.getenv("MAIN_LOOP_SLEEP", "10"))
GDELT_INTERVAL = int(os.getenv("GDELT_INTERVAL", "120"))
REDDIT_INTERVAL = int(os.getenv("REDDIT_INTERVAL", "300"))
NEWSAPI_INTERVAL = int(os.getenv("NEWSAPI_INTERVAL", "900"))

MIN_SCORE = int(os.getenv("MIN_SCORE", "5"))
DIGEST_LIMIT = int(os.getenv("DIGEST_LIMIT", "8"))
DIGEST_COOLDOWN = int(os.getenv("DIGEST_COOLDOWN", "60"))

SEEN_FILE = Path(os.getenv("SEEN_FILE", "seen_hashes.json"))
MAX_SEEN = int(os.getenv("MAX_SEEN", "8000"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

ENABLE_X_STREAM = os.getenv("ENABLE_X_STREAM", "false").lower() == "true"
ENABLE_RSS = os.getenv("ENABLE_RSS", "true").lower() == "true"

X_SEARCH_INTERVAL = int(os.getenv("X_SEARCH_INTERVAL", "120"))
RSS_INTERVAL = int(os.getenv("RSS_INTERVAL", "120"))
# =====================
# LOGGING
# =====================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("GEO_WORKER")


# =====================
# KEYWORDS
# =====================

KEYWORDS = [
    "hormuz",
    "strait of hormuz",
    "persian gulf",
    "iran missile",
    "iran fires",
    "iran attack",
    "iranian missile",
    "us navy",
    "u.s. navy",
    "us warship",
    "u.s. warship",
    "oil tanker",
    "tanker attack",
    "middle east conflict",
    "fars news",
    "red sea",
    "houthis",
]

SOURCE_BOOST = {
    "X": 1,
    "GDELT": 2,
    "NewsAPI": 3,
    "Reddit": 1,
}

# =====================
# MARKET MOVING ENGINE
# =====================

P1_INSTANT_KEYS = [
    # Trump direct / near-direct
    "truth social",
    "trump posted",
    "trump wrote",
    "trump writes",
    "trump announced",
    "trump orders",
    "trump threatens",
    "@realdonaldtrump",

    # War declaration / ceasefire
    "declares war",
    "declaration of war",
    "ceasefire",
    "temporary ceasefire",
    "truce",
    "peace talks",
    "peace proposal",
    "pause operation",
]

P2_MILITARY_KEYS = [
    "exchange fire",
    "exchanged fire",
    "missile strike",
    "drone strike",
    "air strike",
    "warship hit",
    "warship attacked",
    "under attack",
    "sinking",
    "shot down",
    "shoot down",
    "intercepted missile",
    "intercepted drone",
    "us strikes iran",
    "iran strikes us",
    "iran attacks us",
    "us attacks iran",
    "centcom confirms",
    "centcom denies",
]

P3_OIL_HORMUZ_KEYS = [
    "strait of hormuz",
    "hormuz closure",
    "strait closure",
    "blockade",
    "naval blockade",
    "oil tanker attacked",
    "tanker attacked",
    "shipping disruption",
    "reopen the strait",
    "fujairah",
    "oil port",
]

SOFT_SUPPRESS_KEYS = [
    "analysis",
    "analyzing",
    "opinion",
    "commentary",
    "explainer",
    "what to know",
    "what we know",
    "interview",
    "podcast",
    "editorial",
    "column",
    "timeline",
    "background",
]
# =====================
# MEMORY
# =====================

seen_hashes = set()
seen_order = []

x_buffer = []
buffer_lock = threading.Lock()

pending_items = []
pending_lock = threading.Lock()
instant_pending = []
instant_last_sent = 0
instant_lock = threading.Lock()

INSTANT_COOLDOWN = int(os.getenv("INSTANT_COOLDOWN", "180"))
last_sent_at = 0


# =====================
# UTILS
# =====================
# =====================
# PRO OUTPUT MODULE
# =====================

TRUST_HIGH = [
    "reuters", "ap", "associated press", "bloomberg", "bbc", "cnbc",
    "channelnewsasia", "channelnewsasia.com", "cna"
]

TRUST_MEDIUM = [
    "al jazeera", "guardian", "cnn", "globalsecurity",
    "capitalgazette", "leadertelegram"
]
from datetime import datetime, timezone


MAX_NEWS_AGE_MINUTES = int(
    os.getenv("MAX_NEWS_AGE_MINUTES", "30")
)

def is_fresh_item(item):
    pub = item.get("published_at")

    # Nhiều RSS không có giờ chuẩn → không được auto loại
    if not pub:
        item["age_min"] = "unknown"
        return True

    try:
        pub_dt = parser.parse(pub)

        if not pub_dt.tzinfo:
            pub_dt = pub_dt.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        age_min = (now - pub_dt).total_seconds() / 60

        item["age_min"] = round(age_min, 1)

        return age_min <= MAX_NEWS_AGE_MINUTES

    except Exception:
        item["age_min"] = "parse_error"
        return True

def get_trust_level(item):
    raw = item.get("raw_source", "").lower()

    if any(x in raw for x in TRUST_HIGH):
        return "✅ HIGH TRUST"
    if any(x in raw for x in TRUST_MEDIUM):
        return "⚠️ MEDIUM TRUST"
    return "❓ LOW TRUST"
def matched_keys(item, keys):
    text = full_text(item)
    return [k for k in keys if k in text]


def get_market_moving_keys(item):
    p1 = matched_keys(item, P1_INSTANT_KEYS)
    p2 = matched_keys(item, P2_MILITARY_KEYS)
    p3 = matched_keys(item, P3_OIL_HORMUZ_KEYS)
    return p1, p2, p3
def add_instant_item(item):
    global instant_last_sent

    with instant_lock:
        instant_pending.append(item)


def flush_instant_if_ready(force=False):
    global instant_last_sent

    now = time.time()

    with instant_lock:
        if not instant_pending:
            return

        if not force and now - instant_last_sent < INSTANT_COOLDOWN:
            return

        items = list(instant_pending)
        instant_pending.clear()

    items = sorted(items, key=lambda x: x.get("score", 0), reverse=True)
    items = items[:5]

    msg = build_digest(items, alert_type="INSTANT")

    if msg and send_telegram(msg):
        instant_last_sent = now
        save_seen()
        logger.info("[INSTANT_DIGEST] sent=%s", len(items))

def is_instant_market_alert(item):
    p1, p2, p3 = get_market_moving_keys(item)

    # Trump / ceasefire / war declaration: gửi ngay
    if p1:
        return True

    # Mỹ - Iran bắn nhau / bắn hạ / tấn công: gửi ngay
    if p2:
        return True

    # Hormuz/oil shock chỉ gửi ngay nếu đi kèm military hoặc source mạnh
    if p3:
        trust = get_trust_level(item)
        if "HIGH TRUST" in trust:
            return True

    return False


def is_soft_suppressed(item):
    text = full_text(item)

    # Nếu có key cực mạnh thì không suppress
    if is_instant_market_alert(item):
        return False

    return any(k in text for k in SOFT_SUPPRESS_KEYS)


def build_quick_take(items):
    lines = []
    all_text = " ".join([full_text(i) for i in items])

    p1_all, p2_all, p3_all = [], [], []

    for item in items:
        p1, p2, p3 = get_market_moving_keys(item)
        p1_all += p1
        p2_all += p2
        p3_all += p3

    if any(k in all_text for k in ["truth social", "trump posted", "trump wrote", "trump writes", "@realdonaldtrump"]):
        lines.append("Trump/Truth Social có phát ngôn trực tiếp → ưu tiên cực cao")

    if any(k in all_text for k in ["trump announced", "trump orders", "trump threatens"]):
        lines.append("Trump có tuyên bố/hành động chính sách mới → market dễ phản ứng mạnh")

    if any(k in all_text for k in ["exchange fire", "exchanged fire"]):
        lines.append("Mỹ/Iran có dấu hiệu giao tranh trực tiếp")

    if any(k in all_text for k in ["shot down", "shoot down", "sinking", "warship hit", "warship attacked"]):
        lines.append("Có tin bắn hạ/tấn công tàu hoặc khí tài quân sự")

    if any(k in all_text for k in ["missile strike", "drone strike", "air strike"]):
        lines.append("Có tin tấn công tên lửa/drone/không kích")

    if any(k in all_text for k in ["declares war", "declaration of war"]):
        lines.append("Có ngôn ngữ tuyên chiến / leo thang cực mạnh")

    if any(k in all_text for k in ["ceasefire", "truce", "peace talks", "peace proposal", "pause operation"]):
        lines.append("Có yếu tố hoà bình/tạm ngừng bắn → có thể làm dầu/vàng hạ nhiệt")

    if any(k in all_text for k in ["hormuz closure", "strait closure", "blockade", "naval blockade"]):
        lines.append("Rủi ro phong toả Hormuz / gián đoạn vận tải dầu")

    if any(k in all_text for k in ["oil tanker attacked", "tanker attacked", "shipping disruption", "fujairah", "oil port"]):
        lines.append("Rủi ro tanker/cảng dầu/chuỗi cung ứng năng lượng")

    has_high = any(
        "HIGH TRUST" in get_trust_level(i)
        for i in items
    )

    if not has_high:
        lines.append("CHƯA có xác nhận từ Reuters/AP/Bloomberg/BBC/CNA")

    if not lines:
        lines.append("Tin địa chính trị liên quan Hormuz/Iran/Mỹ nhưng chưa có trigger cực mạnh")

    return lines[:5]


def build_market_bias(items):
    text = " ".join([full_text(i) for i in items])

    gold = "➖ Neutral"
    oil = "➖ Neutral"
    btc = "➖ Neutral"
    usd = "➖ Neutral"

    if "hormuz" in text:
        gold = "⬆️ Tăng mạnh"
        oil = "🚀 Tăng rất mạnh"
        btc = "⚠️ Biến động mạnh"
        usd = "⬆️ Tăng"

    if any(k in text for k in ["missile", "attack", "warship"]):
        gold = "🚀 Tăng mạnh"
        oil = "⬆️ Tăng"
        btc = "⬇️ Dễ giảm trước"
        usd = "⬆️ Tăng mạnh"

    return gold, oil, btc, usd


def detect_cluster(items):
    text = " ".join([full_text(i) for i in items])

    if "hormuz" in text:
        return "Hormuz escalation"
    if "iran" in text:
        return "Iran geopolitical tension"

    return "Geopolitical event"



# =====================
# ACTION DECISION
# =====================

def build_action(items):
    text = " ".join([full_text(i) for i in items])

    # check nguồn uy tín
    has_high = any(
        any(x in i.get("raw_source", "").lower() for x in TRUST_HIGH)
        for i in items
    )

    has_attack = any(k in text for k in ["missile", "attack", "strike", "warship"])
    has_hormuz = "hormuz" in text

    # ===== LOGIC =====

    if has_attack and has_high:
        return "EXECUTE (đã confirm)"

    if has_hormuz and not has_high:
        return "WAIT (chưa confirm)"

    if has_hormuz and has_high:
        return "PREPARE (có thể vào)"

    return "WAIT (chưa rõ)"
# =====================
# ACTION BY ASSET (PRO)
# =====================

def build_action_by_asset(items):
    text = " ".join([full_text(i) for i in items])

    # kiểm tra nguồn uy tín
    has_high = any(
        any(x in i.get("raw_source", "").lower() for x in TRUST_HIGH)
        for i in items
    )

    has_attack = any(k in text for k in ["missile", "attack", "strike", "warship"])
    has_hormuz = "hormuz" in text
    has_opec = "opec" in text

    # ===== XAU =====
    if has_attack and has_high:
        xau = "BUY MẠNH"
    elif has_hormuz:
        xau = "BUY (chờ confirm)"
    else:
        xau = "WAIT"

    # ===== OIL =====
    if has_hormuz and not has_opec:
        oil = "BUY MẠNH"
    elif has_hormuz and has_opec:
        oil = "SCALE BUY (xung đột tin)"
    else:
        oil = "WAIT"

    # ===== BTC =====
    if has_attack:
        btc = "SELL NGẮN HẠN (cẩn thận trap)"
    elif has_hormuz:
        btc = "WAIT (dễ nhiễu)"
    else:
        btc = "WAIT"

    return xau, oil, btc

# =====================
# =====================
# FLASH FLAG
# =====================

def build_flash_flag(items):
    direct_post_keys = [
        "truth social",
        "@realdonaldtrump",
        "trump posted",
        "trump wrote",
        "trump says on truth social",
        "trump said on truth social",
        "trump posts",
        "trump post",
    ]

    mention_only_keys = [
        "trump says",
        "trump said",
        "president trump says",
        "president trump said",
    ]

    for item in items:
        text = full_text(item)
        source = item.get("source", "").lower()
        raw = item.get("raw_source", "").lower()

        # Chỉ ưu tiên cao nếu là Telegram OSINT/RSS bắt lại bài đăng trực tiếp
        if any(k in text for k in direct_post_keys):
            return "🚨 FLASH: TRUMP DIRECT POST / TRUTH SOCIAL IMPACT"

        # Nếu chỉ là báo nói "Trump says..." thì flag nhẹ hơn, không gọi là direct post
        if any(k in text for k in mention_only_keys):
            return "⚡ FLASH: TRUMP MENTIONED BY NEWS SOURCE"

    return None
def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalize_item(source, title, text="", url="", raw_source="", published_at=""):
    return {
        "source": source,
        "title": (title or "").strip(),
        "text": (text or "").strip(),
        "url": (url or "").strip(),
        "raw_source": (raw_source or "").strip(),
        "published_at": (published_at or "").strip(),
    }


def full_text(item):
    return f"{item.get('title', '')} {item.get('text', '')}".lower()


def is_relevant(item):
    text = full_text(item)
    return any(k in text for k in KEYWORDS)
def normalize_title(title):
    title = (title or "").lower()
    title = re.sub(r"http\S+", "", title)
    title = re.sub(r"[^a-z0-9\s]", " ", title)
    title = re.sub(r"\s+", " ", title)
    return title.strip()

def make_hash(item):
    title_key = normalize_title(item.get("title", ""))
    source_key = (item.get("raw_source") or item.get("source") or "").lower().strip()

    key = f"{source_key}|{title_key}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def load_seen():
    global seen_hashes, seen_order

    try:
        if SEEN_FILE.exists():
            data = json.loads(SEEN_FILE.read_text())
            seen_order = data[-MAX_SEEN:]
            seen_hashes = set(seen_order)
            logger.info("[DEDUP] loaded=%s", len(seen_hashes))
    except Exception as e:
        logger.warning("[DEDUP] load failed: %s", e)
        seen_hashes = set()
        seen_order = []


def save_seen():
    try:
        data = seen_order[-MAX_SEEN:]
        SEEN_FILE.write_text(json.dumps(data))
    except Exception as e:
        logger.warning("[DEDUP] save failed: %s", e)


def is_duplicate(item):
    h = make_hash(item)

    if h in seen_hashes:
        return True

    seen_hashes.add(h)
    seen_order.append(h)

    if len(seen_order) > MAX_SEEN:
        old = seen_order.pop(0)
        seen_hashes.discard(old)

    return False


def score_item(item):
    text = full_text(item)
    score = 0

    if "hormuz" in text:
        score += 5
    if "strait of hormuz" in text:
        score += 4
    if "persian gulf" in text:
        score += 2

    if "missile" in text:
        score += 4
    if "us navy" in text or "u.s. navy" in text:
        score += 4
    if "warship" in text:
        score += 4
    if "oil tanker" in text or "tanker" in text:
        score += 3
    if "attack" in text or "strike" in text:
        score += 3
    if "explosion" in text:
        score += 3
    if "blockade" in text or "closed" in text:
        score += 5
    if "confirmed" in text:
        score += 2
    if "breaking" in text:
        score += 2

    score += SOURCE_BOOST.get(item.get("source", ""), 0)

    raw = item.get("raw_source", "").lower()
    credible = ["reuters", "ap", "associated press", "bloomberg", "bbc", "al jazeera", "cnbc"]
    if any(c in raw for c in credible):
        score += 4

    return score


def alert_level(score):
    if score >= 15:
        return "🔥 BREAKING GEO ALERT"
    if score >= 11:
        return "🚨 HIGH RISK"
    if score >= 8:
        return "🟠 WATCH FAST"
    return "🟡 WATCH"


# =====================
# TELEGRAM
# =====================

def send_telegram(text):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.error("[TELEGRAM] missing TELEGRAM_TOKEN or CHAT_ID")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    try:
        r = requests.post(
            url,
            json={
                "chat_id": CHAT_ID,
                "text": text[:3900],
                "disable_web_page_preview": True,
            },
            timeout=20,
        )

        if r.status_code != 200:
            logger.error("[TELEGRAM] status=%s text=%s", r.status_code, r.text)
            return False

        return True

    except Exception as e:
        logger.exception("[TELEGRAM] error: %s", e)
        return False


# =====================
# FORMAT DIGEST
# =====================

def build_digest(items, alert_type="NORMAL"):
    if not items:
        return None

    items = sorted(items, key=lambda x: x["score"], reverse=True)
    top_items = items[:3]
    if alert_type == "INSTANT":
        msg += "⚡ TYPE: INSTANT MARKET-MOVING EVENT\n\n"
    else:
        msg += "🧾 TYPE: NORMAL GEO DIGEST\n\n"
    msg = "🔥 BREAKING GEO ALERT\n\n"       
    flash = build_flash_flag(items)
    if flash:
        msg += f"{flash}\n"
        msg += "⚡ Priority: TELEGRAM > RSS > GDELT\n\n"
    # QUICK TAKE
    quick = build_quick_take(items)

    msg += "🧠 QUICK TAKE:\n"
    for q in quick:
        msg += f"• {q}\n"

    # MARKET
    gold, oil, btc, usd = build_market_bias(items)

    msg += "\n📊 MARKET BIAS:\n"
    msg += f"🟡 Gold: {gold}\n"
    msg += f"🛢 Oil: {oil}\n"
    msg += f"₿ BTC: {btc}\n"
    msg += f"💵 USD: {usd}\n"

    # ACTION BY ASSET
    xau, oil, btc = build_action_by_asset(items)
    
    msg += "\n🎯 ACTION (Ref):\n"
    msg += f"🟡 XAU: {xau}\n"
    msg += f"🛢 OIL: {oil}\n"
    msg += f"₿ BTC: {btc}\n"
    # CLUSTER
    cluster = detect_cluster(items)
    msg += f"\n📦 CLUSTER: {cluster} ({len(items)} sources)\n\n"

    # ITEMS
    for i, item in enumerate(top_items, 1):
        trust = get_trust_level(item)

        msg += f"{i}. {alert_level(item['score'])} | Risk Score: {item['score']}\n"
        msg += f"Source: {item.get('raw_source', '')} {trust}\n"
        msg += f"Title: {item.get('title', '')[:200]}\n"

        if item.get("url"):
            msg += f"Link: {item.get('url')}\n"

        msg += "\n"

    # NOTE
    has_high = any(
        any(x in i.get("raw_source", "").lower() for x in TRUST_HIGH)
        for i in items
    )

    msg += "⚠️ Note:\n"
    if not has_high:
        msg += "Chưa có xác nhận từ Reuters/AP/Bloomberg/BBC/CNA → cẩn trọng fake / bias"
    else:
        msg += "Có nguồn uy tín đưa tin, nhưng vẫn cần xác nhận phản ứng giá trước khi vào lệnh."

    return msg
def build_instant_alert(item):
    item = dict(item)
    if "score" not in item:
        item["score"] = score_item(item)

    return build_digest([item])
def add_candidates(items):
    good = []

    for item in items:
        if not item.get("title"):
            continue

        if not is_relevant(item):
            continue
        if not is_fresh_item(item):
            logger.info(
                "[STALE] skipped old news age=%s title=%s",
                item.get("age_min"),
                item.get("title", "")[:80]
            )
            continue
        if is_duplicate(item):
            continue

        if is_soft_suppressed(item):
            logger.info("[SUPPRESS] skipped soft article: %s", item.get("title", "")[:120])
            continue

        score = score_item(item)
        item["score"] = score

        # MODE 1: tin cực mạnh → gửi ngay, không chờ gom
        if is_instant_market_alert(item):
            logger.info("[INSTANT_QUEUE] queued: %s", item.get("title", "")[:120])
            add_instant_item(item)
            continue

        # MODE 2: tin thường → gom, nhưng siết điểm để giảm spam
        if score < max(MIN_SCORE, 12):
            continue

        good.append(item)

    if good:
        with pending_lock:
            pending_items.extend(good)

    return len(good)


def flush_digest_if_ready(force=False):
    global last_sent_at

    now = time.time()

    if not force and now - last_sent_at < DIGEST_COOLDOWN:
        return

    with pending_lock:
        if not pending_items:
            return

        items = list(pending_items)
        pending_items.clear()

    items.sort(key=lambda x: x["score"], reverse=True)
    items = items[:DIGEST_LIMIT]

    digest = build_digest(items, alert_type="NORMAL")

    if digest and send_telegram(digest):
        last_sent_at = now
        save_seen()
        logger.info("[DIGEST] sent=%s", len(items))


# =====================
# FETCH GDELT
# =====================

def fetch_gdelt():
    if not ENABLE_GDELT:
        return []

    url = "https://api.gdeltproject.org/api/v2/doc/doc"

    params = {
        "query": '(Hormuz OR "Strait of Hormuz" OR "Iran missile" OR "US Navy" OR "oil tanker")',
        "mode": "ArtList",
        "maxrecords": 20,
        "format": "json",
        "sort": "DateDesc",
    }

    try:
        r = requests.get(url, params=params, timeout=20)

        if r.status_code != 200:
            logger.warning("[GDELT] status=%s text=%s", r.status_code, r.text[:200])
            return []

        data = r.json()
        articles = data.get("articles", [])

        items = [
            normalize_item(
                source="GDELT",
                title=a.get("title"),
                text=a.get("seendate", ""),
                url=a.get("url"),
                raw_source=a.get("domain", ""),
                published_at=a.get("seendate", ""),
            )
            for a in articles
        ]

        logger.info("[GDELT] fetched=%s", len(items))
        return items

    except Exception as e:
        logger.exception("[GDELT] error: %s", e)
        return []


# =====================
# FETCH NEWSAPI
# =====================

def fetch_newsapi():
    if not ENABLE_NEWSAPI or not NEWSAPI_KEY:
        return []

    url = "https://newsapi.org/v2/everything"

    params = {
        "q": '"Strait of Hormuz" OR Hormuz OR "Iran missile" OR "US Navy" OR "oil tanker"',
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 20,
        "apiKey": NEWSAPI_KEY,
    }

    try:
        r = requests.get(url, params=params, timeout=20)
        data = r.json()

        if r.status_code != 200:
            logger.warning("[NEWSAPI] status=%s data=%s", r.status_code, data)
            return []

        articles = data.get("articles", [])

        items = [
            normalize_item(
                source="NewsAPI",
                title=a.get("title"),
                text=a.get("description") or a.get("content"),
                url=a.get("url"),
                raw_source=(a.get("source") or {}).get("name", ""),
                published_at=a.get("publishedAt", ""),
            )
            for a in articles
        ]

        logger.info("[NEWSAPI] fetched=%s", len(items))
        return items

    except Exception as e:
        logger.exception("[NEWSAPI] error: %s", e)
        return []

# =====================
# TELEGRAM OSINT SOURCE
# =====================

from telethon import TelegramClient

tg_client = None
tg_buffer = []
tg_last_id = {}

def init_telegram_client():
    global tg_client

    if not ENABLE_TG_SOURCE:
        return

    try:
        tg_client = TelegramClient(
            "tg_session",
            int(os.getenv("TG_API_ID")),
            os.getenv("TG_API_HASH"),
        )

        tg_client.start()
        logger.info("[TG] client started")

    except Exception as e:
        logger.error("[TG] init error: %s", e)


def fetch_telegram_channels():
    if not ENABLE_TG_SOURCE or not tg_client:
        return []

    channels = os.getenv("TG_CHANNELS", "").split(",")
    items = []

    for ch in channels:
        ch = ch.strip()
        if not ch:
            continue

        try:
            last_id = tg_last_id.get(ch, 0)

            messages = tg_client.loop.run_until_complete(
                tg_client.get_messages(ch, limit=5)
            )

            for msg in messages:
                if not msg.text:
                    continue

                if msg.id <= last_id:
                    continue

                items.append(
                    normalize_item(
                        source="TELEGRAM",
                        title=msg.text[:200],
                        text=msg.text,
                        url=f"https://t.me/{ch}/{msg.id}",
                        raw_source=ch,
                        published_at=str(msg.date),
                    )
                )

                tg_last_id[ch] = max(tg_last_id.get(ch, 0), msg.id)

        except Exception as e:
            logger.warning("[TG] error channel=%s err=%s", ch, e)

    logger.info("[TG] fetched=%s", len(items))
    return items
# =====================
# FETCH REDDIT
# =====================

def fetch_reddit():
    if not ENABLE_REDDIT:
        return []

    if not praw:
        logger.warning("[REDDIT] praw not installed")
        return []

    if not REDDIT_CLIENT_ID or not REDDIT_SECRET:
        return []

    try:
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_SECRET,
            user_agent=REDDIT_USER_AGENT,
        )

        subs = [
            "worldnews",
            "geopolitics",
            "credibledefense",
            "LessCredibleDefence",
        ]

        query = 'Hormuz OR "Strait of Hormuz" OR "Iran missile" OR "US Navy" OR "oil tanker"'
        items = []

        for sub in subs:
            try:
                for post in reddit.subreddit(sub).search(
                    query,
                    sort="new",
                    time_filter="day",
                    limit=10,
                ):
                    items.append(
                        normalize_item(
                            source="Reddit",
                            title=post.title,
                            text=getattr(post, "selftext", "")[:600],
                            url=f"https://reddit.com{post.permalink}",
                            raw_source=f"r/{sub}",
                            published_at=datetime.fromtimestamp(
                                post.created_utc,
                                tz=timezone.utc,
                            ).strftime("%Y-%m-%d %H:%M:%S UTC"),
                        )
                    )
            except Exception as e:
                logger.warning("[REDDIT] sub=%s error=%s", sub, e)

        logger.info("[REDDIT] fetched=%s", len(items))
        return items

    except Exception as e:
        logger.exception("[REDDIT] error: %s", e)
        return []


# =====================
# X STREAM
# =====================

class GeoXStream(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        item = normalize_item(
            source="X",
            title=tweet.text,
            text=tweet.text,
            url=f"https://x.com/i/web/status/{tweet.id}",
            raw_source="Filtered Stream",
            published_at=utc_now(),
        )

        with buffer_lock:
            x_buffer.append(item)

        logger.info("[X] tweet buffered")

    def on_connection_error(self):
        logger.warning("[X] connection error")
        self.disconnect()

    def on_exception(self, exception):
        logger.exception("[X] exception: %s", exception)
        time.sleep(15)

    def on_errors(self, errors):
        logger.warning("[X] errors=%s", errors)


def start_x_stream():
    if not ENABLE_X:
        logger.info("[X] disabled")
        return

    if not tweepy:
        logger.warning("[X] tweepy not installed")
        return

    if not TWITTER_BEARER:
        logger.warning("[X] missing TWITTER_BEARER")
        return

    while True:
        try:
            stream = GeoXStream(TWITTER_BEARER, wait_on_rate_limit=True)

            rules = stream.get_rules()
            if rules and rules.data:
                stream.delete_rules([rule.id for rule in rules.data])

            rule = (
                'Hormuz OR "Strait of Hormuz" OR "Iran missile" OR '
                '"US Navy" OR "oil tanker" OR "Persian Gulf"'
            )

            stream.add_rules(tweepy.StreamRule(rule))

            logger.info("[X] stream started")
            stream.filter(tweet_fields=["created_at"])

        except Exception as e:
            logger.exception("[X] stream crashed: %s", e)
            time.sleep(30)
def fetch_x_search():
    if not ENABLE_X or not TWITTER_BEARER:
        return []

    if not tweepy:
        logger.warning("[X_SEARCH] tweepy not installed")
        return []

    query = (
        '(Hormuz OR "Strait of Hormuz" OR "Persian Gulf" OR '
        '"Iran missile" OR "US Navy" OR CENTCOM OR warship OR tanker) '
        '-is:retweet lang:en'
    )

    try:
        client = tweepy.Client(
            bearer_token=TWITTER_BEARER,
            wait_on_rate_limit=True
        )

        resp = client.search_recent_tweets(
            query=query,
            max_results=10,
            tweet_fields=["created_at", "author_id"]
        )

        tweets = resp.data or []
        items = []

        for t in tweets:
            items.append(
                normalize_item(
                    source="X",
                    title=t.text,
                    text=t.text,
                    url=f"https://x.com/i/web/status/{t.id}",
                    raw_source="X Search",
                    published_at=str(t.created_at) if getattr(t, "created_at", None) else utc_now(),
                )
            )

        logger.info("[X_SEARCH] fetched=%s", len(items))
        return items

    except Exception as e:
        logger.exception("[X_SEARCH] error: %s", e)
        return []
def fetch_rss_feeds():
    if not ENABLE_RSS:
        return []

    queries = [
        'site:reuters.com Hormuz OR "Strait of Hormuz" OR "Iran missile" OR "US Navy"',
        'site:aljazeera.com Hormuz OR "Strait of Hormuz" OR "Iran missile" OR "US Navy"',
        'site:bbc.com Hormuz OR "Strait of Hormuz" OR "Iran missile" OR "US Navy"',
        'site:cnbc.com Hormuz OR "Strait of Hormuz" OR "Iran missile" OR "US Navy"',
    ]

    items = []

    for q in queries:
        encoded = urllib.parse.quote(q)
        url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"

        try:
            feed = feedparser.parse(url)

            for entry in feed.entries[:10]:
                source_title = ""

                if hasattr(entry, "source"):
                    source_title = getattr(entry.source, "title", "")

                items.append(
                    normalize_item(
                        source="RSS",
                        title=entry.get("title", ""),
                        text=entry.get("summary", ""),
                        url=entry.get("link", ""),
                        raw_source=source_title or "Google News RSS",
                        published_at=entry.get("published", ""),
                    )
                )

            logger.info("[RSS] query=%s fetched=%s", q[:30], len(feed.entries))

        except Exception as e:
            logger.warning("[RSS] error query=%s err=%s", q, e)

    return items
def drain_x_buffer():
    with buffer_lock:
        items = list(x_buffer)
        x_buffer.clear()

    return items


# =====================
# MAIN WORKER
# =====================

def run_worker():
    global last_sent_at

    load_seen()
    #init_telegram_client()
    logger.info("===== GEO REALTIME WORKER STARTED =====")
    logger.info(
        "X=%s | X_STREAM=%s | RSS=%s | GDELT=%s | REDDIT=%s | NEWSAPI=%s",
        ENABLE_X, ENABLE_X_STREAM, ENABLE_RSS, ENABLE_GDELT, ENABLE_REDDIT, ENABLE_NEWSAPI
    )
    logger.info("Intervals: GDELT=%ss | Reddit=%ss | NewsAPI=%ss", GDELT_INTERVAL, REDDIT_INTERVAL, NEWSAPI_INTERVAL)

    if ENABLE_X and ENABLE_X_STREAM and TWITTER_BEARER:
        t = threading.Thread(target=start_x_stream, daemon=True)
        t.start()
    else:
        logger.info("[X] stream disabled, using X search polling")

    last_gdelt = 0
    last_reddit = 0
    last_newsapi = 0
    last_x_search = 0
    last_rss = 0
    
    while True:
        try:
            now = time.time()
            all_items = []
            #if ENABLE_TG_SOURCE:
                #all_items += fetch_telegram_channels()
            # Realtime X buffer
            all_items += drain_x_buffer()
            if ENABLE_X and not ENABLE_X_STREAM and now - last_x_search >= X_SEARCH_INTERVAL:
                all_items += fetch_x_search()
                last_x_search = now
            
            if ENABLE_RSS and now - last_rss >= RSS_INTERVAL:
                all_items += fetch_rss_feeds()
                last_rss = now
            # Smart polling
            if ENABLE_GDELT and now - last_gdelt >= GDELT_INTERVAL:
                all_items += fetch_gdelt()
                last_gdelt = now

            if ENABLE_REDDIT and now - last_reddit >= REDDIT_INTERVAL:
                all_items += fetch_reddit()
                last_reddit = now

            if ENABLE_NEWSAPI and now - last_newsapi >= NEWSAPI_INTERVAL:
                all_items += fetch_newsapi()
                last_newsapi = now

            added = add_candidates(all_items)

            if added > 0:
                logger.info("[CANDIDATES] added=%s", added)

            flush_digest_if_ready()
            flush_instant_if_ready()
            logger.info("[ALIVE] worker alive | seen=%s", len(seen_hashes))

        except Exception as e:
            logger.exception("[MAIN] error: %s", e)

        time.sleep(MAIN_LOOP_SLEEP)


if __name__ == "__main__":
    run_worker()
