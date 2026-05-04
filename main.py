import os
import time
import json
import hashlib
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

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
# MEMORY
# =====================

seen_hashes = set()
seen_order = []

x_buffer = []
buffer_lock = threading.Lock()

pending_items = []
pending_lock = threading.Lock()

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

def get_trust_level(item):
    raw = item.get("raw_source", "").lower()

    if any(x in raw for x in TRUST_HIGH):
        return "✅ HIGH TRUST"
    if any(x in raw for x in TRUST_MEDIUM):
        return "⚠️ MEDIUM TRUST"
    return "❓ LOW TRUST"


def build_quick_take(items):
    text = " ".join([full_text(i) for i in items])
    lines = []

    if "hormuz" in text:
        lines.append("Iran đang siết kiểm soát Hormuz")

    if "tanker" in text or "oil" in text:
        lines.append("Nguy cơ gián đoạn nguồn cung dầu")

    if any(k in text for k in ["missile", "attack", "warship"]):
        lines.append("Có dấu hiệu leo thang quân sự")

    has_high = any(
        any(x in i.get("raw_source", "").lower() for x in TRUST_HIGH)
        for i in items
    )

    if not has_high:
        lines.append("CHƯA có xác nhận từ Reuters/AP")

    return lines


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


def make_hash(item):
    key = f"{item.get('title', '')}|{item.get('url', '')}|{item.get('source', '')}"
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

def build_digest(items):
    if not items:
        return None

    items = sorted(items, key=lambda x: x["score"], reverse=True)
    top_items = items[:3]

    msg = "🔥 BREAKING GEO ALERT\n\n"

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

def add_candidates(items):
    good = []

    for item in items:
        if not item.get("title"):
            continue

        if not is_relevant(item):
            continue

        if is_duplicate(item):
            continue

        score = score_item(item)

        if score < MIN_SCORE:
            continue

        item["score"] = score
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

    digest = build_digest(items)

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

    logger.info("===== GEO REALTIME WORKER STARTED =====")
    logger.info("X=%s | GDELT=%s | REDDIT=%s | NEWSAPI=%s", ENABLE_X, ENABLE_GDELT, ENABLE_REDDIT, ENABLE_NEWSAPI)
    logger.info("Intervals: GDELT=%ss | Reddit=%ss | NewsAPI=%ss", GDELT_INTERVAL, REDDIT_INTERVAL, NEWSAPI_INTERVAL)

    if ENABLE_X and TWITTER_BEARER:
        t = threading.Thread(target=start_x_stream, daemon=True)
        t.start()

    last_gdelt = 0
    last_reddit = 0
    last_newsapi = 0

    while True:
        try:
            now = time.time()
            all_items = []

            # Realtime X buffer
            all_items += drain_x_buffer()

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

            logger.info("[ALIVE] worker alive | seen=%s", len(seen_hashes))

        except Exception as e:
            logger.exception("[MAIN] error: %s", e)

        time.sleep(MAIN_LOOP_SLEEP)


if __name__ == "__main__":
    run_worker()
