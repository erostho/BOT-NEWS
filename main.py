# app/main.py
from __future__ import annotations
import json
import os
import time
import asyncio
import logging
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse
from app.data_source import get_candles, ingest_mt5_candles
from app.pro_analysis import analyze_pro, format_signal, build_scale_plan_v2, format_scale_plan_v2, get_now_status, _setup_class_score_v3, build_view_engine_v1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

app = FastAPI(title="xau-liquidity-bot")

# ---- Simple concurrency guards for cron ----
CRON_LOCK = asyncio.Lock()
LAST_CRON_TS = 0
MIN_CRON_GAP_SEC = int(os.getenv("MIN_CRON_GAP_SEC", "25"))

# Default symbols (override by env SYMBOLS="XAU/USD,BTC/USD,XAG/USD")
DEFAULT_SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "XAU/USD,BTC/USD").split(",") if s.strip()]

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")  # default chat for cron
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", TELEGRAM_CHAT_ID)

REGIME_ALERT_ENABLED=1
REGIME_CHOP_THRESHOLD=6.8
REGIME_ALERT_COOLDOWN_MIN=120
REGIME_ALERT_STATE_PATH = os.getenv("REGIME_ALERT_STATE_PATH", "regime_alert_state.json")


CRON_SECRET = os.getenv("CRON_SECRET", "")

# Send both symbols always by default (you can override)
MIN_STARS = int(os.getenv("MIN_STARS", "1"))

# Telegram hard limit is 4096; keep safe chunk size
TG_CHUNK = int(os.getenv("TG_CHUNK", "3500"))

@app.get("/health")
def health():
    return {"status": "ok"}
def _send_telegram(text: str, chat_id: Optional[str] = None) -> None:
    token = TELEGRAM_TOKEN
    cid = chat_id or TELEGRAM_CHAT_ID
    if not token or not cid:
        logger.info("[TG] missing TELEGRAM_TOKEN/CHAT_ID -> skip sending")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests = __import__("requests")
        payload = {
            "chat_id": cid,
            "text": str(text or ""),
            "disable_web_page_preview": True,
        }
        resp = requests.post(url, json=payload, timeout=15)
        logger.info("[TG] send status=%s body=%s", resp.status_code, resp.text)
    except Exception as e:
        logger.exception("[TG] send failed: %s", e)
        
def _send_long_telegram(text: str, chat_id: str, chunk_size: int = 3500, parse_mode=None):
    text = str(text or "")
    if not text.strip():
        return

    parts = []
    buf = ""

    for line in text.splitlines(True):  # giữ newline
        if len(buf) + len(line) <= chunk_size:
            buf += line
        else:
            if buf:
                parts.append(buf)
            if len(line) <= chunk_size:
                buf = line
            else:
                # line quá dài thì cắt cứng
                for i in range(0, len(line), chunk_size):
                    parts.append(line[i:i + chunk_size])
                buf = ""

    if buf:
        parts.append(buf)

    total = len(parts)
    for i, part in enumerate(parts, start=1):
        header = f"📩 REVIEW ({i}/{total})\n" if total > 1 else ""
        _send_telegram(header + part, chat_id=chat_id)
        
    
def _parse_symbol_from_text(text: str) -> str:
    t = text.lower()

    if "xag" in t or "silver" in t:
        return "XAG/USD"

    if "xau" in t or "gold" in t:
        return "XAU/USD"

    if "btc" in t:
        return "BTC/USD"

    return DEFAULT_SYMBOLS[0] if DEFAULT_SYMBOLS else "XAU/USD"

def _is_scale_command(text: str) -> bool:
    t = (text or "").strip().upper()
    return t in (
        "BTC SCALE", "XAU SCALE", "XAG SCALE",
        "BTC/USD SCALE", "XAU/USD SCALE", "XAG/USD SCALE"
    )


import re

# 1) Chuẩn hoá symbol người dùng gõ -> symbol hệ thống dùng để get_candles()
def normalize_symbol(user_sym: str) -> str:
    s = (user_sym or "").strip().upper()
    s = s.replace("-", "/").replace("_", "/").replace(" ", "")
    # cho phép: BTC, BTCUSD, BTC/USD, BTCUSDT...
    if s in ("BTC", "BTCUSD", "BTC/USD", "BTCUSDT", "BTC/USDT"):
        return "BTC/USD"
    if s in ("XAU", "XAUUSD", "XAU/USD"):
        return "XAU/USD"
    if s in ("XAG", "XAGUSD", "XAG/USD"):
        return "XAG/USD"

    # fallback: nếu user gõ kiểu BTC/USDT -> BTC/USD (mày đang dùng USD)
    if s.endswith("/USDT"):
        s = s.replace("/USDT", "/USD")
    if s.endswith("USDT"):
        s = s.replace("USDT", "/USD")
    if "/" not in s and s.endswith("USD"):
        s = s[:-3] + "/USD"
    return s


# 2) Parse lệnh manual: hỗ trợ entry 1 giá hoặc 1 vùng (a-b), TP/SL có/không
_MANUAL_RE = re.compile(
    r"^\s*(?P<sym>[A-Za-z\/\-_]+)\s+"
    r"(?P<side>BUY|SELL)\s+"
    r"(?P<entry1>\d+(\.\d+)?)"
    r"(?:\s*[-~]\s*(?P<entry2>\d+(\.\d+)?))?"
    r"(?:\s+(?:TP|TP1|TARGET)\s*[:=\s]+(?P<tp>\d+(\.\d+)?))?"
    r"(?:\s+(?:SL|STOP)\s*[:=\s]+(?P<sl>\d+(\.\d+)?))?"
    r"\s*$",
    re.IGNORECASE
)

def parse_manual_trade(text: str):
    m = _MANUAL_RE.match((text or "").strip())
    if not m:
        return None

    symbol = normalize_symbol(m.group("sym"))
    side = m.group("side").upper()

    e1 = float(m.group("entry1"))
    e2 = float(m.group("entry2")) if m.group("entry2") else None
    entry_lo = min(e1, e2) if e2 is not None else e1
    entry_hi = max(e1, e2) if e2 is not None else e1

    tp = float(m.group("tp")) if m.group("tp") else None
    sl = float(m.group("sl")) if m.group("sl") else None

    return {
        "symbol": symbol,
        "side": side,
        "entry_lo": entry_lo,
        "entry_hi": entry_hi,
        "tp": tp,
        "sl": sl,
    }

def _as_list_from_get_candles(res):
    """
    get_candles() của mày có lúc trả:
      - list
      - (list, meta)
    -> normalize về list
    """
    if isinstance(res, tuple) and len(res) >= 1:
        return res[0] or []
    return res or []


def _as_list_and_source_from_get_candles(res):
    """Unwrap return of get_candles(): (candles, source) or candles."""
    source = None
    candles = res
    if isinstance(res, tuple) and len(res) == 2:
        candles, source = res
    if candles is None:
        return [], source
    try:
        return list(candles), source
    except Exception:
        return [], source

def _cget(c, k, default=0.0):
    if isinstance(c, dict):
        v = c.get(k, default)
    else:
        v = getattr(c, k, default)
    try:
        return float(v)
    except Exception:
        return float(default)

def _m15_closed(candles):
    # bỏ nến đang chạy (nếu có)
    if not candles:
        return []
    return candles[:-1] if len(candles) > 1 else candles

def _atr14_simple(candles):
    """
    ATR(14) đơn giản (Wilder) để review lệnh.
    candles: list dict/object OHLC (đã đóng là tốt nhất)
    """
    cs = candles or []
    if len(cs) < 16:
        return None
    trs = []
    for i in range(1, len(cs)):
        h = _cget(cs[i], "high")
        l = _cget(cs[i], "low")
        pc = _cget(cs[i-1], "close")
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < 14:
        return None
    atr = sum(trs[:14]) / 14.0
    for j in range(14, len(trs)):
        atr = (atr * 13.0 + trs[j]) / 14.0
    return atr

def _range30_info_m15(m15):
    closed = _m15_closed(m15)
    use = closed[-30:] if len(closed) >= 30 else closed
    if len(use) < 12:
        return None
    lo = min(_cget(x, "low") for x in use)
    hi = max(_cget(x, "high") for x in use)
    cur = _cget(use[-1], "close")
    rng = max(1e-9, hi - lo)
    pos = (cur - lo) / rng  # 0..1
    return {"lo": lo, "hi": hi, "cur": cur, "pos": pos, "use": use}

def _hl_lh_gate(m15, atr_val):
    """
    “CHỜ CẤU TRÚC” nghĩa là gì? -> trả text rõ:
    - BUY: M15 tạo Higher-Low (HL) + sau đó đóng vượt đỉnh gần (break high gần)
    - SELL: M15 tạo Lower-High (LH) + sau đó đóng thủng đáy gần (break low gần)
    """
    closed = _m15_closed(m15)
    if len(closed) < 22:
        return {"hl": False, "lh": False, "break_up": False, "break_dn": False, "txt": "Chưa đủ dữ liệu M15 để xác nhận cấu trúc."}

    # cushion theo ATR để tránh nhiễu
    a = float(atr_val or 0.0)
    cushion = max(1e-9, 0.12 * a) if a > 0 else 0.0

    last5 = closed[-5:]
    prev5 = closed[-10:-5]

    low_last5 = min(_cget(x, "low") for x in last5)
    low_prev5 = min(_cget(x, "low") for x in prev5)
    hl = low_last5 > (low_prev5 + cushion)

    high_last5 = max(_cget(x, "high") for x in last5)
    high_prev5 = max(_cget(x, "high") for x in prev5)
    lh = high_last5 < (high_prev5 - cushion)

    # “break” mốc gần: dùng high/low của 10 nến trước đó (không tính 1-2 nến cuối)
    ref = closed[-12:-2]
    ref_hi = max(_cget(x, "high") for x in ref)
    ref_lo = min(_cget(x, "low") for x in ref)
    cur_close = _cget(closed[-1], "close")

    break_up = cur_close > (ref_hi + 0.05 * a if a > 0 else ref_hi)
    break_dn = cur_close < (ref_lo - 0.05 * a if a > 0 else ref_lo)

    txt = (
        f"Gate cấu trúc:\n"
        f"- BUY chỉ mạnh khi: HL=True và M15 đóng > đỉnh gần ({ref_hi:.2f}).\n"
        f"- SELL chỉ mạnh khi: LH=True và M15 đóng < đáy gần ({ref_lo:.2f})."
    )
    return {"hl": hl, "lh": lh, "break_up": break_up, "break_dn": break_dn, "ref_hi": ref_hi, "ref_lo": ref_lo, "txt": txt}

def _trade_management_5_10_15(
    side: str,
    entry: float,
    cur: float | None,
    atr_val: float | None,
    gate: dict | None,
    div: dict | None,
    cpat: dict | None,
    volq: dict | None,
    tp: float | None,
    sl: float | None,
) -> dict:
    side = (side or "").upper().strip()
    gate = gate or {}
    div = div or {}
    cpat = cpat or {}
    volq = volq or {}

    a = float(atr_val or 0.0)
    if cur is None or a <= 0:
        return {
            "stage": "5",
            "label": "Validation",
            "lines": [
                "Chưa đủ dữ liệu giá hiện tại / ATR để tính 5-10-15.",
                "Tạm coi đang ở pha kiểm tra ban đầu, ưu tiên giữ nhỏ và không add."
            ],
        }

    move = (float(cur) - float(entry)) if side == "BUY" else (float(entry) - float(cur)
)
    r_mult = move / max(a, 1e-9)

    lines = []

    # ===== Stage 5: Validation =====
    if r_mult < 0.8:
        lines.append("5: Lệnh còn ở pha kiểm tra → chỉ giữ ngắn hạn, chưa add.")
        if volq.get("state") == "LOW":
            lines.append("Volume thấp → nếu có lời nên ưu tiên chốt nhanh TP1.")
        if side == "BUY" and div.get("bear"):
            lines.append("Có bearish divergence chống BUY → tránh gồng.")
        if side == "SELL" and div.get("bull"):
            lines.append("Có bullish divergence chống SELL → tránh gồng.")
        return {"stage": "5", "label": "Validation", "lines": lines}

    # ===== Stage 10: Expansion =====
    if r_mult < 1.2:
        lines.append("10: Lệnh bắt đầu có lợi thế → cân nhắc dời SL về BE.")
        if side == "BUY":
            if gate.get("hl") or gate.get("break_up"):
                lines.append("BUY có HL / break_up hỗ trợ → có thể giữ thêm.")
            else:
                lines.append("BUY chưa có break_up rõ → vẫn không add.")
        else:
            if gate.get("lh") or gate.get("break_dn"):
                lines.append("SELL có LH / break_dn hỗ trợ → có thể giữ thêm.")
            else:
                lines.append("SELL chưa có break_dn rõ → vẫn không add.")
        return {"stage": "10", "label": "Expansion", "lines": lines}

    # ===== Stage 15: Harvest / Continuation =====
    lines.append("15: Lệnh đã chạy đủ xa → ưu tiên bảo vệ lợi nhuận.")
    lines.append("Nên trailing theo high/low 3 nến M15 hoặc chốt từng phần.")
    if tp is not None:
        lines.append(f"TP hiện tại đang đặt: {float(tp):.2f}")
    if sl is not None:
        lines.append(f"SL hiện tại đang đặt: {float(sl):.2f}")
    if cpat.get("rejection") in ("UPPER", "LOWER"):
        lines.append("Có rejection candle gần đây → cân nhắc harvest bớt vị thế.")
    return {"stage": "15", "label": "Harvest", "lines": lines}

def build_review_decision_engine_v2(
    side,
    pos,
    gate,
    ex_state,
    trigger_state,
    entry=None,
    cur=None,
):
    side = str(side or "NONE").upper()
    ex_state = str(ex_state or "WAIT").upper()
    trigger_state = str(trigger_state or "WAIT").upper()
    gate = gate or {}

    if ex_state == "EXIT_NOW":
        decision = "EXIT"
    elif ex_state == "REDUCE_RISK":
        decision = "REDUCE"
    elif ex_state in ("HOLD", "HOLD_LIGHT"):
        decision = "HOLD_NO_ADD"
    elif ex_state == "CUT_SOON":
        decision = "REDUCE_OR_EXIT"
    else:
        decision = "WAIT"

    entry_status = "UNKNOWN"
    try:
        rp = float(pos)
        if side == "SELL":
            if rp < 0.25:
                entry_status = "TRỄ / SELL thấp"
            elif rp > 0.70:
                entry_status = "ĐẸP / gần vùng cao"
            else:
                entry_status = "GIỮA RANGE"
        elif side == "BUY":
            if rp > 0.75:
                entry_status = "TRỄ / BUY cao"
            elif rp < 0.30:
                entry_status = "ĐẸP / gần vùng thấp"
            else:
                entry_status = "GIỮA RANGE"
    except Exception:
        pass

    if decision not in ("HOLD_NO_ADD", "HOLD"):
        add_action = "❌ Không add"
    elif trigger_state == "TRIGGERED":
        add_action = "⚠️ Có thể add rất nhẹ nếu đúng plan"
    else:
        add_action = "❌ Không add, chưa có trigger"

    wait_conditions = []
    wrong_reasons = []

    if side == "SELL":
        if not gate.get("lh"):
            wait_conditions.append("Cần có LH rõ")
            wrong_reasons.append("SELL chưa có LH")
        if not gate.get("break_dn"):
            wait_conditions.append("Cần break low / giữ dưới")
    elif side == "BUY":
        if not gate.get("hl"):
            wait_conditions.append("Cần có HL rõ")
            wrong_reasons.append("BUY chưa có HL")
        if not gate.get("break_up"):
            wait_conditions.append("Cần break high / giữ trên")

    try:
        rp = float(pos)
        if side == "SELL" and rp < 0.20:
            wrong_reasons.append("SELL ở vùng thấp, dễ bán đuổi")
        if side == "BUY" and rp > 0.80:
            wrong_reasons.append("BUY ở vùng cao, dễ mua đuổi")
    except Exception:
        pass

    risk_note = []
    try:
        entry_f = float(entry)
        cur_f = float(cur)
        if side == "SELL":
            if cur_f < entry_f:
                risk_note.append("Lệnh đang có lời → ưu tiên bảo vệ lợi nhuận")
            else:
                risk_note.append("Lệnh đang âm/chưa có lợi thế → không add")
        elif side == "BUY":
            if cur_f > entry_f:
                risk_note.append("Lệnh đang có lời → ưu tiên bảo vệ lợi nhuận")
            else:
                risk_note.append("Lệnh đang âm/chưa có lợi thế → không add")
    except Exception:
        pass

    return {
        "decision": decision,
        "entry_status": entry_status,
        "add_action": add_action,
        "wait_conditions": wait_conditions[:4],
        "wrong_reasons": list(dict.fromkeys(wrong_reasons))[:4],
        "risk_note": risk_note[:3],
    }

def review_manual_trade(symbol: str, side: str, entry_lo: float, entry_hi: float, tp: float | None, sl: float | None) -> str:
    symbol = str(symbol or "").strip().upper()
    side = (side or "").upper().strip()

    def _f(x, nd: int = 2, default: str = "n/a") -> str:
        try:
            if x is None:
                return default
            return f"{float(x):.{nd}f}"
        except Exception:
            return default

    def _pct(x, default: str = "n/a") -> str:
        try:
            return f"{int(round(float(x) * 100))}%"
        except Exception:
            return default
    def _safe_float(x, default: float = 0.0) -> float:
        try:
            return float(x) if x is not None else default
        except Exception:
            return default
    try:
        entry_lo = float(entry_lo)
        entry_hi = float(entry_hi)
        entry = (entry_lo + entry_hi) / 2.0
    except Exception as e:
        logger.exception("Invalid entry values for manual review %s %s: %s", symbol, side, e)
        return f"❌ REVIEW lỗi cho {symbol}: entry không hợp lệ."

    try:
        m15, src15 = _as_list_and_source_from_get_candles(get_candles(symbol, "15min", limit=220))
        m30, src30 = _as_list_and_source_from_get_candles(get_candles(symbol, "30min", limit=220))
        h1, src1h = _as_list_and_source_from_get_candles(get_candles(symbol, "1h", limit=220))
        h4, src4h = _as_list_and_source_from_get_candles(get_candles(symbol, "4h", limit=220))
    except Exception as e:
        logger.exception("get_candles failed for %s: %s", symbol, e)
        return f"❌ REVIEW lỗi cho {symbol}: không lấy được dữ liệu nến ({e})."

    try:
        current_price = _cget(m15[-1], "close", None) if m15 else None
        sig = analyze_pro(
            symbol,
            m15,
            m30,
            h1,
            h4,
            current_price=current_price,
        )
        if not isinstance(sig, dict):
            sig = {
                "symbol": symbol, "tf": "M30", "session": "", "recommendation": "CHỜ",
                "stars": 1, "trade_mode": "MANUAL", "meta": {},
                "context_lines": ["Hệ phân tích chưa trả đủ signal → fallback review."],
                "liquidity_lines": [], "quality_lines": [], "notes": [],
            }
    except Exception as e:
        logger.exception("analyze_pro failed for %s: %s", symbol, e)
        sig = {
            "symbol": symbol, "tf": "M30", "session": "", "recommendation": "CHỜ",
            "stars": 1, "trade_mode": "MANUAL", "meta": {},
            "context_lines": [f"Hệ phân tích lỗi → fallback review ({e})."],
            "liquidity_lines": [], "quality_lines": [], "notes": [],
        }

    try:
        ds = src30 or src15 or src1h or src4h
        if ds:
            sig["data_source"] = ds
            sig.setdefault("meta", {})["data_source"] = ds
    except Exception:
        pass

    meta = sig.get("meta", {}) or {}
    if not isinstance(meta, dict):
        meta = {}

    volq = meta.get("volq", {}) or {}
    cpat = meta.get("candle", {}) or {}
    div = meta.get("div", {}) or {}
    phase369 = meta.get("phase_369", {}) or {}
    flow_state = meta.get("flow_state", {}) or {}
    liquidation = meta.get("liquidation", {}) or {}
    no_trade_zone = meta.get("no_trade_zone", {}) or {}
    market_state_v2 = meta.get("market_state_v2")
    playbook = meta.get("playbook_v2", {}) or {}
    narrative_v3 = meta.get("narrative_v3", {}) or {}
    scenario_v3 = meta.get("scenario_v3", {}) or {}
    session_v4 = meta.get("session_v4", {}) or {}
    htf_pressure_v4 = meta.get("htf_pressure_v4", {}) or {}
    close_confirm_v4 = meta.get("close_confirm_v4", {}) or {}
    macro_v4 = meta.get("macro_v4", {}) or {}
    playbook_v4 = meta.get("playbook_v4", {}) or {}
    ema_pack = sig.get("ema") or meta.get("ema") or {}

    ctx = sig.get("context_lines", []) or []
    liq = sig.get("liquidity_lines", []) or []
    qlt = sig.get("quality_lines", []) or []
    notes = sig.get("notes", []) or []

    atr_val = None
    for x in qlt:
        s = str(x)
        if "ATR(" in s and "~" in s:
            try:
                atr_val = float(s.split("~")[-1].strip())
                break
            except Exception:
                atr_val = None
    if atr_val is None:
        atr_val = _atr14_simple(_m15_closed(m15)) or 0.0
    a = _safe_float(atr_val, 0.0)

    rr_txt = "RR: n/a"
    try:
        if tp is not None and sl is not None and abs(entry - float(sl)) > 1e-9:
            risk = abs(entry - float(sl))
            reward = abs(float(tp) - entry)
            rr_txt = f"RR≈{(reward / risk):.2f}"
    except Exception:
        pass

    rinfo = _range30_info_m15(m15)
    cur = rinfo["cur"] if isinstance(rinfo, dict) else None
    pos = rinfo["pos"] if isinstance(rinfo, dict) else None
    lo = rinfo["lo"] if isinstance(rinfo, dict) else None
    hi = rinfo["hi"] if isinstance(rinfo, dict) else None

    try:
        gate = _hl_lh_gate(m15, atr_val) or {}
    except Exception:
        gate = {"txt": "Không đọc được gate cấu trúc.", "hl": False, "lh": False, "break_up": False, "break_dn": False}

    actions = []
    vol_state = str((volq or {}).get("state") or "").upper()
    vol_ratio = _safe_float((volq or {}).get("ratio"), 0.0)
    if vol_state and vol_state != "N/A":
        actions.append(f"📦 Volume: {vol_state} (x{vol_ratio:.2f} vs SMA20)")
        if vol_state == "LOW":
            actions.append("⚠️ Volume thấp → ưu tiên TP nhanh, KHÔNG add.")
        elif vol_state == "HIGH":
            actions.append("✅ Volume cao → move đáng tin hơn, có thể giữ theo plan.")

    cpat_txt = str((cpat or {}).get("txt") or "").strip()
    if cpat_txt and cpat_txt != "N/A":
        actions.append(f"🕯 Candle: {cpat_txt}")

    div_txt = str((div or {}).get("txt") or "").strip()
    if div_txt and div_txt != "N/A":
        actions.append(f"📉 {div_txt}")

    if pos is not None:
        pos_pct = int(max(0, min(1, float(pos))) * 100)
        if side == "BUY" and float(pos) > 0.70:
            actions.append(f"⚠️ Entry đang ~{pos_pct}% range M15 → BUY dễ bị xem là đuổi.")
        if side == "SELL" and float(pos) < 0.30:
            actions.append(f"⚠️ Entry đang ~{pos_pct}% range M15 → SELL dễ bị xem là đuổi.")

    h1_txt = " ".join(str(x).lower() for x in ctx)
    if side == "BUY" and "h1: bearish" in h1_txt:
        actions.append("⚠️ BUY ngược H1 bearish → ưu tiên đánh ngắn, không gồng.")
    if side == "SELL" and "h1: bullish" in h1_txt:
        actions.append("⚠️ SELL ngược H1 bullish → ưu tiên đánh ngắn, không gồng.")

    if a > 0:
        if sl is not None:
            sl_atr = abs(entry - float(sl)) / a
            actions.append((f"⚠️ SL hơi sát: ~{sl_atr:.2f} ATR → dễ dính quét.") if sl_atr < 0.70 else f"✅ SL khoảng ~{sl_atr:.2f} ATR → tạm ổn.")
        if tp is not None:
            tp_atr = abs(float(tp) - entry) / a
            actions.append((f"⚠️ TP hơi ngắn: ~{tp_atr:.2f} ATR.") if tp_atr < 0.70 else f"✅ TP khoảng ~{tp_atr:.2f} ATR.")
        if sl is not None and tp is not None:
            try:
                rr_now = abs(float(tp) - entry) / max(abs(entry - float(sl)), 1e-9)
                if sl_atr < 0.70 and rr_now >= 3.0:
                    actions.append("⚠️ RR đẹp nhưng SL khá sát → vẫn dễ bị quét trước khi đi đúng hướng")
            except Exception:
                pass

    verdict = "TRUNG TÍNH"
    if cur is not None and a > 0:
        move = (float(cur) - entry) if side == "BUY" else (entry - float(cur))
        dd_atr = (-move) / a
        if side == "BUY":
            if gate.get("hl") and gate.get("break_up"):
                verdict = "ĐÚNG (cấu trúc ủng hộ)"
                actions.append("✅ Đã có HL + break đỉnh gần → có thể GIỮ.")
            elif dd_atr >= 0.80 and not gate.get("hl"):
                verdict = "SAI/NGUY HIỂM"
                actions.append("🛑 Đang âm mạnh >0.8 ATR mà chưa có HL → ưu tiên THOÁT hoặc giảm size.")
            else:
                verdict = "CHƯA RÕ"
                actions.append("⏳ Có thể giữ ngắn hạn, KHÔNG add. Chờ break xác nhận.")
        else:
            if gate.get("lh") and gate.get("break_dn"):
                verdict = "ĐÚNG (cấu trúc ủng hộ)"
                actions.append("✅ Đã có LH + break đáy gần → có thể GIỮ.")
            elif dd_atr >= 0.80 and not gate.get("lh"):
                verdict = "SAI/NGUY HIỂM"
                actions.append("🛑 Đang âm mạnh >0.8 ATR mà chưa có LH → ưu tiên THOÁT hoặc giảm size.")
            else:
                verdict = "CHƯA RÕ"
                actions.append("⏳ Có thể giữ ngắn hạn, KHÔNG add. Chờ break xác nhận.")

    if isinstance(no_trade_zone, dict) and no_trade_zone.get("active"):
        actions.append("🚨 Đang ở no-trade zone → không nên add hoặc mở rộng rủi ro.")

    try:
        mgmt = _trade_management_5_10_15(side, entry, cur, a, gate, div, cpat, volq, tp, sl) or {}
    except Exception as e:
        mgmt = {"stage": "n/a", "label": "fallback", "lines": [f"Không tính được 5-10-15: {e}"]}

    tp1_s = tp2_s = sl_s = None
    if a > 0:
        if side == "BUY":
            tp1_s = entry + 0.90 * a
            tp2_s = entry + 1.80 * a
            sl_s = entry - 1.10 * a
        else:
            tp1_s = entry - 0.90 * a
            tp2_s = entry - 1.80 * a
            sl_s = entry + 1.10 * a


    def _vn_phase_label(pobj: dict) -> str:
        p = str((pobj or {}).get("label") or "").upper()
        return {
            "EARLY": "Giai đoạn sớm",
            "READY": "Có thể chuẩn bị",
            "LATE": "Đang ở đoạn muộn",
            "EXTREME": "Biến động quá mạnh",
            "BOUNCE_TO_SELL": "Hồi để bán",
            "DIP_TO_BUY": "Hồi để mua",
        }.get(p, p or "Chưa rõ")

    def _vn_state_text(state, narrative, side):
        state = str(state or "").upper()
    
        # QUAN TRỌNG: gắn với lệnh
        if side == "SELL" and state in ["TREND_UP", "PULLBACK_UP", "DIP_TO_BUY"]:
            return "Thị trường đang nghiêng tăng → lệnh SELL đang ngược xu hướng, chỉ nên đánh ngắn"
    
        if side == "BUY" and state in ["TREND_DOWN", "PULLBACK_DOWN", "BOUNCE_TO_SELL"]:
            return "Thị trường đang nghiêng giảm → lệnh BUY đang ngược xu hướng, chỉ nên đánh ngắn"
    
        return narrative.get("summary") or state

        # REVIEW ưu tiên ngữ cảnh của lệnh đang cầm, không chỉ copy narrative thị trường
        if order_side == "SELL" and state in ("TREND_UP", "PULLBACK_UP", "DIP_TO_BUY"):
            return "Bối cảnh lớn vẫn nghiêng tăng; lệnh SELL hiện đang ngược hướng chính, chỉ nên đánh ngắn."
        if order_side == "BUY" and state in ("TREND_DOWN", "PULLBACK_DOWN", "BOUNCE_TO_SELL"):
            return "Bối cảnh lớn vẫn nghiêng giảm; lệnh BUY hiện đang ngược hướng chính, chỉ nên đánh ngắn."

        mapping = {
            "TREND_DOWN": "Xu hướng giảm đang chiếm ưu thế",
            "PULLBACK_DOWN": "Đang hồi trong xu hướng giảm",
            "BOUNCE_TO_SELL": "Đang hồi lên trong bối cảnh giảm; ưu tiên chờ hồi yếu để canh bán",
            "TREND_UP": "Xu hướng tăng đang chiếm ưu thế",
            "PULLBACK_UP": "Đang điều chỉnh trong xu hướng tăng",
            "DIP_TO_BUY": "Đang điều chỉnh trong bối cảnh tăng; ưu tiên chờ giữ đáy để canh mua",
            "CHOP": "Thị trường nhiễu, dễ quét hai đầu",
            "TRANSITION": "Thị trường đang chuyển trạng thái, chưa nên vội vào lệnh",
            "POST_LIQUIDATION_BOUNCE": "Sau cú quét mạnh, thị trường dễ hồi kỹ thuật",
            "POST_SHORT_COVER": "Sau cú ép thoát lệnh bán, thị trường dễ hồi mạnh",
            "EXHAUSTION_DOWN": "Đà giảm đang có dấu hiệu yếu dần",
            "EXHAUSTION_UP": "Đà tăng đang có dấu hiệu yếu dần",
        }
        return summary or mapping.get(state, state or "Chưa rõ")

    def _vn_flow_text(flow_obj: dict) -> str:
        state = str((flow_obj or {}).get("state") or "").upper()
        favored = str((flow_obj or {}).get("favored_side") or "").upper()
        mapping = {
            "INFLOW": "Dòng tiền đang vào",
            "OUTFLOW": "Dòng tiền đang ra",
            "RISK_ON": "Dòng tiền đang nghiêng về tài sản rủi ro",
            "RISK_OFF": "Dòng tiền đang rời tài sản rủi ro",
            "NEUTRAL": "Dòng tiền trung tính",
        }
        base = mapping.get(state, state or "Chưa rõ")
        if favored == "BUY":
            return f"{base} | Ưu tiên BUY"
        if favored == "SELL":
            return f"{base} | Ưu tiên SELL"
        return f"{base} | Ưu tiên NONE"
    def _review_confidence_risk(final_score: int, tradeable_label: str, side: str, pos, ntz_obj: dict, htf_obj: dict, liquidation_obj: dict) -> tuple[str, str]:
        confidence = "LOW"
        if tradeable_label == "YES" and final_score >= 75:
            confidence = "HIGH"
        elif final_score >= 55:
            confidence = "MEDIUM"

        risk_points = 0
        if isinstance(ntz_obj, dict) and ntz_obj.get("active"):
            risk_points += 2
        if isinstance(liquidation_obj, dict) and liquidation_obj.get("ok"):
            risk_points += 2
        htf_state = str((htf_obj or {}).get("state") or "").upper()
        if str(side).upper() == "BUY" and "BEARISH" in htf_state:
            risk_points += 2
        if str(side).upper() == "SELL" and "BULLISH" in htf_state:
            risk_points += 2
        try:
            rp = float(pos)
            if str(side).upper() == "BUY" and rp > 0.80:
                risk_points += 2
            elif str(side).upper() == "SELL" and rp < 0.20:
                risk_points += 2
            elif 0.30 <= rp <= 0.70:
                risk_points += 1
        except Exception:
            pass

        if risk_points >= 5:
            risk = "HIGH"
        elif risk_points >= 3:
            risk = "MEDIUM"
        else:
            risk = "LOW"
        return confidence, risk
    def _grade_from_verdict(v: str) -> str:
        vv = str(v or "").upper()
        if "ĐÚNG" in vv:
            return "A"
        if "CHƯA RÕ" in vv:
            return "B"
        if "SAI" in vv or "NGUY HIỂM" in vv:
            return "C"
        return "B"
    def _review_market_vs_position(side: str, verdict_text: str, grade_text: str, ntz_obj: dict) -> str:
        """
        REVIEW phải trả lời cho lệnh đang cầm, không bê nguyên playbook NOW.
        """
        if grade_text == "A":
            return "Lệnh hiện tại vẫn có thể giữ theo kế hoạch, nhưng không nên mở thêm rủi ro mới"
        if grade_text == "B":
            return "Có thể giữ ngắn hạn nếu vẫn đúng cấu trúc, nhưng không nên add"
        if ntz_obj.get("active"):
            return "Thị trường chưa đẹp để mở mới; nếu đang có lệnh thì chỉ quản trị chặt"
        return "Ưu tiên giảm rủi ro và chờ cấu trúc rõ hơn"

    def _review_main_plan(side: str, scenario_obj: dict, playbook_obj: dict, grade_text: str, verdict_text: str, ntz_obj: dict) -> str:
        base = str((scenario_obj or {}).get("base_case") or "").strip()
        base_upper = base.upper()

        if "NO TRADE" in base_upper or "ĐỨNG NGOÀI" in base_upper:
            if grade_text == "A":
                return "Không nên mở lệnh mới, nhưng lệnh hiện tại vẫn có thể giữ ngắn hạn nếu tiếp tục đúng cấu trúc"
            if grade_text == "B":
                return "Không nên mở lệnh mới; nếu đang có lệnh thì chỉ giữ nhỏ và không add"
            return "Ưu tiên đứng ngoài, không nên giữ thêm nếu lệnh đang yếu"

        base = base.replace("Base case:", "").strip()
        base = base.replace("hồi để SELL", "chờ hồi để canh bán")
        base = base.replace("hồi để BUY", "chờ điều chỉnh để canh mua")

        zlo = playbook_obj.get("zone_low")
        zhi = playbook_obj.get("zone_high")
        if zlo is not None and zhi is not None:
            zlo_txt = _f(zlo)
            zhi_txt = _f(zhi)
            if zlo_txt not in base and zhi_txt not in base:
                base = f"{base} trong vùng {zlo_txt} – {zhi_txt}"

        return base or "Chưa có kịch bản chính rõ"

    def _review_invalidation(side: str, sl_val, gate_obj: dict) -> str:
        if side == "SELL":
            if gate_obj.get("lh"):
                return f"Nếu vượt {_f(sl_val)} hoặc mất LH → kịch bản SELL bị yếu đi rõ"
            return f"Nếu vượt {_f(sl_val)} hoặc tiếp tục không tạo được LH → kịch bản SELL chưa đủ mạnh"
        else:
            if gate_obj.get("hl"):
                return f"Nếu thủng {_f(sl_val)} hoặc mất HL → kịch bản BUY bị yếu đi rõ"
            return f"Nếu thủng {_f(sl_val)} hoặc tiếp tục không giữ được HL → kịch bản BUY chưa đủ mạnh"
    def _review_location_override(pos, side):
        try:
            rp = float(pos)
            side = str(side).upper()
            if rp < 0.20:
                if side == "SELL":
                    return {
                        "zone": "LOW_ZONE",
                        "state_text": "Đang ở vùng thấp → ưu tiên quan sát phản ứng, tránh SELL đáy.",
                        "action_lines": [
                            "🚫 Vị trí vùng thấp → không nên mở SELL mới.",
                            "⚠️ Nếu đang có SELL: chỉ giữ nhỏ, không add.",
                        ],
                    }
                return {
                    "zone": "LOW_ZONE",
                    "state_text": "Đang ở vùng thấp → ưu tiên chờ phản ứng giữ giá, tránh bán tháo.",
                    "action_lines": [
                        "✅ Vị trí vùng thấp phù hợp hơn cho BUY nếu có xác nhận.",
                        "⚠️ Nếu đang có BUY: chỉ giữ nhỏ cho tới khi có HL / break xác nhận.",
                    ],
                }
            if rp > 0.80:
                if side == "BUY":
                    return {
                        "zone": "HIGH_ZONE",
                        "state_text": "Đang ở vùng cao → ưu tiên quan sát phản ứng, tránh BUY đuổi.",
                        "action_lines": [
                            "🚫 Vị trí vùng cao → không nên mở BUY mới.",
                            "⚠️ Nếu đang có BUY: chỉ giữ nhỏ, không add.",
                        ],
                    }
                return {
                    "zone": "HIGH_ZONE",
                    "state_text": "Đang ở vùng cao → vị trí thuận lợi hơn cho SELL nếu có xác nhận.",
                    "action_lines": [
                        "✅ Vị trí vùng cao phù hợp hơn cho SELL nếu có LH / từ chối rõ.",
                        "⚠️ Nếu đang có SELL: chỉ giữ theo plan, không add khi chưa break.",
                    ],
                }
        except Exception:
            pass
        return {"zone": None, "state_text": None, "action_lines": []}

    def _final_score_review(side: str, gate: dict, pos, actions, playbook_obj: dict, ntz_obj: dict, htf_obj: dict):
        score = 50
        reasons = []
        htf_state = str((htf_obj or {}).get("state") or "").upper()
        side_u = str(side).upper()

        if "STRONG" in htf_state:
            score += 8
            reasons.append("HTF mạnh")
        elif "WEAK" in htf_state:
            score += 4
            reasons.append("HTF hơi nghiêng")
        else:
            score -= 4
            reasons.append("HTF chưa đồng thuận")

        if side_u == "SELL":
            if gate.get("lh"):
                score += 10
                reasons.append("đã có LH")
            else:
                score -= 10
                reasons.append("chưa có LH")
            if gate.get("break_dn"):
                score += 10
                reasons.append("đã phá đáy")
            if "BULLISH" in htf_state:
                score -= 10
                reasons.append("đang ngược khung lớn")
        else:
            if gate.get("hl"):
                score += 10
                reasons.append("đã có HL")
            else:
                score -= 10
                reasons.append("chưa có HL")
            if gate.get("break_up"):
                score += 10
                reasons.append("đã phá đỉnh")
            if "BEARISH" in htf_state:
                score -= 10
                reasons.append("đang ngược khung lớn")

        try:
            rp = float(pos)
            if rp <= 0.20 or rp >= 0.80:
                score += 6
                reasons.append("đang ở vùng biên")
            elif 0.30 <= rp <= 0.70:
                score -= 8
                reasons.append("đang ở giữa biên độ")

            side_u2 = str(side).upper()
            if side_u2 == "SELL" and rp < 0.20:
                score -= 15
                reasons.append("SELL ở vùng thấp")
            if side_u2 == "BUY" and rp > 0.80:
                score -= 15
                reasons.append("BUY ở vùng cao")
        except Exception:
            pass

        if isinstance(ntz_obj, dict) and ntz_obj.get("active"):
            score -= 10
            reasons.append("đang ở no-trade zone")

        action_text = " | ".join(str(x) for x in (actions or [])).upper()
        if "VOLUME CAO" in action_text or "VOLUME: HIGH" in action_text:
            score += 4
            reasons.append("volume ủng hộ")
        elif "VOLUME THẤP" in action_text or "VOLUME: LOW" in action_text:
            score -= 4
            reasons.append("volume yếu")
        if "ENGULFING=BULL" in action_text or "ENGULFING=BEAR" in action_text or "REJECTION=UPPER" in action_text or "REJECTION=LOWER" in action_text:
            score += 3
            reasons.append("nến có phản ứng rõ")

        score = max(0, min(100, score))

        if score >= 75:
            setup_label = "MẠNH"
        elif score >= 55:
            setup_label = "TRUNG BÌNH"
        else:
            setup_label = "YẾU"

        tradeable = True
        trade_reasons = []
        try:
            rp = float(pos)
            if 0.30 <= rp <= 0.70:
                tradeable = False
                trade_reasons.append("đang ở giữa biên độ")
            side_u2 = str(side).upper()
            if side_u2 == "SELL" and rp < 0.20:
                tradeable = False
                trade_reasons.append("SELL đang ở vùng thấp")
            if side_u2 == "BUY" and rp > 0.80:
                tradeable = False
                trade_reasons.append("BUY đang ở vùng cao")
        except Exception:
            pass
        if isinstance(ntz_obj, dict) and ntz_obj.get("active"):
            tradeable = False
            trade_reasons.append("đang ở vùng no-trade")
        if side_u == "SELL" and not gate.get("lh"):
            tradeable = False
            trade_reasons.append("SELL chưa có LH xác nhận")
        if side_u == "BUY" and not gate.get("hl"):
            tradeable = False
            trade_reasons.append("BUY chưa có HL xác nhận")
        if score < 60:
            tradeable = False
            trade_reasons.append("edge chưa đủ mạnh")
        if side_u == "SELL":
            try:
                if float(pos) < 0.20:
                    tradeable = False
                    trade_reasons.append("SELL đang ở vùng thấp")
            except Exception:
                pass
        if side_u == "BUY":
            try:
                if float(pos) > 0.80:
                    tradeable = False
                    trade_reasons.append("BUY đang ở vùng cao")
            except Exception:
                pass

        dedup_reasons=[]; seen=set()
        for r in reasons:
            if r not in seen:
                seen.add(r); dedup_reasons.append(r)
        dedup_trade=[]; seen2=set()
        for r in trade_reasons:
            if r not in seen2:
                seen2.add(r); dedup_trade.append(r)
        return score, ("YES" if tradeable else "NO"), dedup_reasons[:5], dedup_trade[:4], setup_label

    def _extract_gap_lines(ctx_lines, note_lines):
        out = []
        seen = set()
        for raw in list(ctx_lines or []) + list(note_lines or []):
            s = str(raw or "").strip()
            low = s.lower()
            if any(k in low for k in ["gap", "mở cửa", "biên độ đầu phiên", "đầu phiên", "mất cân bằng"]):
                if s not in seen:
                    seen.add(s)
                    out.append(s)
        return out[:3]

    gap_lines = _extract_gap_lines(ctx, notes)
    grade = _grade_from_verdict(verdict)
    side_vn = "MUA" if side == "BUY" else "BÁN"
    location_ctx = _review_location_override(pos, side)

    v = str(verdict or "").upper()
    if "CHƯA RÕ" in v:
        if side == "SELL":
            verdict_text = "Tạm ổn — cùng bối cảnh giảm nhưng chưa có LH xác nhận"
        else:
            verdict_text = "Tạm ổn — cùng bối cảnh tăng nhưng chưa có HL xác nhận"
    elif "ĐÚNG" in v:
        verdict_text = "Ổn — đang đi cùng hướng chính"
    elif "SAI" in v or "NGUY HIỂM" in v:
        verdict_text = "Không ổn — lệnh đang ở trạng thái rủi ro cao"
    else:
        verdict_text = str(verdict or "").strip()

    summary_text = _review_market_vs_position(side, verdict_text, grade, no_trade_zone if isinstance(no_trade_zone, dict) else {})
    final_verdict_text = verdict_text
    if location_ctx.get("zone") == "LOW_ZONE" and str(side).upper() == "SELL":
        final_verdict_text = "Không còn vị trí đẹp để SELL — đang ở vùng thấp, ưu tiên không đuổi lệnh."
        summary_text = "Nếu đang có SELL từ vùng cao hơn thì chỉ giữ nhỏ, không add."
    elif location_ctx.get("zone") == "HIGH_ZONE" and str(side).upper() == "BUY":
        final_verdict_text = "Không còn vị trí đẹp để BUY — đang ở vùng cao, ưu tiên không đuổi lệnh."
        summary_text = "Nếu đang có BUY từ vùng thấp hơn thì chỉ giữ nhỏ, không add."

    final_state_text = location_ctx.get("state_text") or _vn_state_text(market_state_v2, narrative_v3 if isinstance(narrative_v3, dict) else {}, side)

    lines = []
    lines.append(f"🧠 REVIEW LỆNH | {symbol} | {side_vn}")
    lines.append("")
    lines.append(f"📌 Kết luận: {final_verdict_text}")
    lines.append(f"- {summary_text}")

    if phase369:
        lines.append(f"🧭 Giai đoạn: {phase369.get('phase', 'n/a')} | {_vn_phase_label(phase369)}")
    lines.append(f"🌡 Trạng thái: {final_state_text}")
    if isinstance(flow_state, dict) and flow_state.get("state"):
        lines.append(f"💰 Dòng tiền: {_vn_flow_text(flow_state)}")

    lines.append("")
    lines.append("📍 Vị trí giá:")
    if cur is not None:
        lines.append(f"- Giá hiện tại: {_f(cur)}")
    if isinstance(rinfo, dict):
        lines.append(f"- Biên độ M15: {_f(lo)} – {_f(hi)}")
        if pos is not None:
            lines.append(f"- Vị trí trong biên độ: ~{_pct(pos)}")

    lines.append("")
    lines.append("💧 Thanh khoản:")
    for s in (liq[:4] if liq else ["Chưa thấy quét/spring rõ"]):
        lines.append(f"- {s}")
    if isinstance(liquidation, dict) and liquidation.get("ok"):
        lines.append(f"- Vừa có quét mạnh: {liquidation.get('side')} | body~{float(liquidation.get('body_atr', 0) or 0):.1f} ATR | range~{float(liquidation.get('range_atr', 0) or 0):.1f} ATR")

    lines.append("")
    lines.append("✅ Xác nhận:")
    lines.append(f"- HL={'✅' if gate.get('hl') else '❌'} | LH={'✅' if gate.get('lh') else '❌'} | BreakUp={'✅' if gate.get('break_up') else '❌'} | BreakDn={'✅' if gate.get('break_dn') else '❌'}")
    if side == "SELL":
        if location_ctx.get("zone") == "LOW_ZONE":
            lines.append("- Đã có LH nhưng giá đang ở vùng thấp → SELL có logic cấu trúc, nhưng vị trí không còn đẹp.")
        elif not gate.get("lh"):
            lines.append("- Chưa có LH → tín hiệu SELL chưa đủ mạnh, chỉ nên giữ ngắn hạn")
        elif gate.get("lh") and not gate.get("break_dn"):
            lines.append("- Đã có LH nhưng chưa phá đáy → SELL đúng hướng nhưng chưa xác nhận mạnh")
        elif gate.get("lh") and gate.get("break_dn"):
            lines.append("- Đã có LH và phá đáy → tín hiệu SELL đang mạnh hơn")
    elif side == "BUY":
        if location_ctx.get("zone") == "HIGH_ZONE":
            lines.append("- Đã có HL nhưng giá đang ở vùng cao → BUY có logic cấu trúc, nhưng vị trí không còn đẹp.")
        elif not gate.get("hl"):
            lines.append("- Chưa có HL → tín hiệu BUY chưa đủ mạnh, chỉ nên giữ ngắn hạn")
        elif gate.get("hl") and not gate.get("break_up"):
            lines.append("- Đã có HL nhưng chưa phá đỉnh → BUY đúng hướng nhưng chưa xác nhận mạnh.")
        elif gate.get("hl") and gate.get("break_up"):
            lines.append("- Đã có HL và phá đỉnh → tín hiệu BUY đang mạnh hơn")
    lines.append(f"- {gate.get('txt') or 'Chưa đọc được gate cấu trúc.'}")

    flow1 = meta.get("flow_engine_v1") or {}
    lines.append("")
    lines.append("🧠 FLOW / IMBALANCE:")
    lines.append(f"- State: {flow1.get('state', 'NEUTRAL')}")
    lines.append(f"- Displacement: {flow1.get('displacement', 'NONE')}")
    lines.append(f"- Liquidity: {flow1.get('liquidity_state', 'Chưa thấy sweep/spring rõ')}")
    lines.append(f"- Gap: {flow1.get('gap_text', 'Chưa có dấu hiệu GAP / mở cửa bất thường rõ')}")
    lines.append(f"- Imbalance: {flow1.get('fvg_text', 'chưa có vùng rõ')}")
    lines.append(f"- Ý nghĩa: {flow1.get('narrative', 'Flow chưa rõ')}")
    lines.append(f"- Hành động gợi ý: {flow1.get('action_hint', 'WAIT')}")
    
    if ema_pack:
        lines.append("")
        lines.append("📉 EMA FILTER:")
        if ema_pack.get("ema34") is not None:
            lines.append(f"- EMA34: {_f(ema_pack.get('ema34'))}")
        if ema_pack.get("ema89") is not None:
            lines.append(f"- EMA89: {_f(ema_pack.get('ema89'))}")
        if ema_pack.get("ema200") is not None:
            lines.append(f"- EMA200: {_f(ema_pack.get('ema200'))}")
        lines.append(f"- Trend: {ema_pack.get('trend', 'N/A')}")
        lines.append(f"- Alignment: {ema_pack.get('alignment', 'NO')}")
        if ema_pack.get("zone"):
            lines.append(f"- Vị trí giá vs EMA: {ema_pack.get('zone')}")
    lines.append("")
    lines.append("🗺 Kịch bản chính:")
    main_plan_text = _review_main_plan(side, scenario_v3 if isinstance(scenario_v3, dict) else {}, playbook if isinstance(playbook, dict) else {}, grade, verdict_text, no_trade_zone if isinstance(no_trade_zone, dict) else {})
    lines.append(f"- {main_plan_text}")

    lines.append("")
    lines.append("🪄 Kịch bản phụ:")
    if isinstance(scenario_v3, dict) and scenario_v3.get("alt_case"):
        alt = scenario_v3.get("alt_case")
        alt = alt.replace("Alt case:", "").strip()
        alt = alt.replace("break đỉnh mạnh", "vượt đỉnh mạnh")
        alt = alt.replace("breakdown risk", "nguy cơ giảm mạnh")
        alt = alt.replace("reversal candidate", "nguy cơ đổi hướng")
        lines.append(f"- {alt}")
    else:
        lines.append("- Chưa có kịch bản phụ rõ")

    lines.append("")
    lines.append("🧯 Điểm sai kịch bản:")
    lines.append(f"- {_review_invalidation(side, sl, gate)}")

    lines.append("")
    lines.append(f"📊 Chất lượng hiện tại: {grade}")
    final_score, tradeable_label, score_reasons, tradeable_reasons, setup_label = _final_score_review(
        side,
        gate,
        pos,
        actions,
        playbook if isinstance(playbook, dict) else {},
        no_trade_zone if isinstance(no_trade_zone, dict) else {},
        htf_pressure_v4 if isinstance(htf_pressure_v4, dict) else {},
    )
    lines.append(f"🔥 Final Score: {final_score}/100")
    lines.append(f"→ Tradeable thêm: {tradeable_label}")
    if tradeable_label == "NO":
        lines.append("- Vị thế hiện tại chưa đủ đẹp để mở thêm rủi ro mới")
    else:
        lines.append("- Vị thế hiện tại còn đủ điều kiện để giữ theo kế hoạch")
    if score_reasons:
        lines.append(f"- Điểm cộng/trừ chính: {', '.join(score_reasons)}")
    if tradeable_reasons:
        lines.append(f"- Lý do chưa trade mạnh: {', '.join(tradeable_reasons)}")
    lines.append(f"- Độ mạnh setup hiện tại: {setup_label}")
    if score_reasons:
        lines.append(f"- Lý do: {', '.join(score_reasons[:3])}")

    if side == "SELL" and isinstance(htf_pressure_v4, dict):
        htf_state = str(htf_pressure_v4.get("state") or "")
        if "BULLISH" in htf_state:
            lines.append("- ⚠️ SELL chưa được khung lớn ủng hộ hoàn toàn → không nên gồng")
    if side == "BUY" and isinstance(htf_pressure_v4, dict):
        htf_state = str(htf_pressure_v4.get("state") or "")
        if "BEARISH" in htf_state:
            lines.append("- ⚠️ BUY chưa được khung lớn ủng hộ hoàn toàn → không nên gồng")

    psych_warnings = []
    try:
        rp = float(pos)
        if rp <= 0.10 and str(side).upper() == "SELL":
            psych_warnings.append("⚠️ FOMO SELL vùng thấp → dễ đuổi giá")
        elif rp >= 0.90 and str(side).upper() == "BUY":
            psych_warnings.append("⚠️ FOMO BUY vùng cao → dễ đuổi đỉnh")
        if rp >= 0.85 and str(side).upper() == "SELL" and not gate.get("lh"):
            psych_warnings.append("⚠️ SELL vùng cao nhưng chưa có LH → dễ bán sớm")
        if rp <= 0.15 and str(side).upper() == "BUY" and not gate.get("hl"):
            psych_warnings.append("⚠️ BUY vùng thấp nhưng chưa có HL → dễ bắt đáy sớm")
    except Exception:
        pass
    if isinstance(liquidation, dict) and liquidation.get("ok"):
        psych_warnings.append("⚠️ Sau liquidation → market dễ bật ngược / nhiễu mạnh")
    if str(side).upper() == "BUY" and isinstance(htf_pressure_v4, dict) and "BEARISH" in str(htf_pressure_v4.get("state") or "").upper():
        psych_warnings.append("⚠️ Đây là lệnh ngược xu hướng chính → chỉ giữ ngắn hạn")
    if str(side).upper() == "SELL" and isinstance(htf_pressure_v4, dict) and "BULLISH" in str(htf_pressure_v4.get("state") or "").upper():
        psych_warnings.append("⚠️ Đây là lệnh ngược xu hướng chính → chỉ giữ ngắn hạn")
    if "Engulfing=BULL" in " | ".join(actions) and str(side).upper() == "SELL":
        psych_warnings.append("⚠️ Engulfing bull đang chống SELL → tránh gồng")
    if "Engulfing=BEAR" in " | ".join(actions) and str(side).upper() == "BUY":
        psych_warnings.append("⚠️ Engulfing bear đang chống BUY → tránh gồng")
    if psych_warnings:
        lines.append("")
        lines.append("🧠 Cảnh báo tâm lý:")
        for s in psych_warnings[:4]:
            lines.append(f"- {s}")
    lines.append("")
    lines.append("⚙️ Hành động:")
    for s in (location_ctx.get("action_lines") or []):
        lines.append(f"- {s}")

    if location_ctx.get("zone") == "LOW_ZONE" and str(side).upper() == "SELL":
        lines.append("- Ưu tiên giảm rủi ro; nếu đã có SELL từ vùng cao hơn thì chỉ giữ nhỏ, không mở thêm.")
    elif location_ctx.get("zone") == "HIGH_ZONE" and str(side).upper() == "BUY":
        lines.append("- Ưu tiên giảm rủi ro; nếu đã có BUY từ vùng thấp hơn thì chỉ giữ nhỏ, không mở thêm.")
    elif grade == "A":
        lines.append("- Có thể giữ lệnh theo kế hoạch, nhưng không nên mở thêm lệnh mới")
    elif grade == "B":
        lines.append("- Có thể giữ ngắn hạn nếu cấu trúc chưa hỏng, nhưng không nên add")
    else:
        lines.append("- Ưu tiên giảm rủi ro và quan sát thêm")

    filtered_actions = []
    for s in actions:
        txt = str(s)
        if location_ctx.get("zone") == "LOW_ZONE" and str(side).upper() == "SELL":
            if "SELL đúng hướng" in txt or "khung lớn ủng hộ SELL" in txt:
                continue
        if location_ctx.get("zone") == "HIGH_ZONE" and str(side).upper() == "BUY":
            if "BUY đúng hướng" in txt or "khung lớn ủng hộ BUY" in txt:
                continue
        filtered_actions.append(txt)

    seen = set()
    for s in filtered_actions:
        ss = str(s).strip()
        if ss and ss not in seen:
            seen.add(ss)
            lines.append(f"- {ss}")
    lines.append("")
    lines.append("🪜 Quản trị 5-10-15:")
    stage_label_map = {
        "Validation": "Pha kiểm tra",
        "Expansion": "Pha mở rộng",
        "Harvest": "Pha thu lợi nhuận",
    }
    stage_label_vn = stage_label_map.get(str(mgmt.get('label', 'n/a')), str(mgmt.get('label', 'n/a')))
    lines.append(f"- Stage: {mgmt.get('stage', 'n/a')} | {stage_label_vn}")
    for s in (mgmt.get('lines', []) or [])[:4]:
        lines.append(f"- {s}")

    if a > 0:
        lines.append("")
        lines.append("🎯 TP/SL THAM KHẢO THEO VÙNG:")
        # lấy vùng gần từ M15 range / gate
        near_support = lo
        near_resistance = hi
        try:
            move_atr = ((entry - cur) / a) if side == "SELL" else ((cur - entry) / a)
        except Exception:
            move_atr = 0.0
        if side == "SELL":
            # ===== FIX TP ORDER =====
            candidates = [x for x in [near_support, tp1_s, tp2_s] if x is not None]
            if candidates:
                candidates_sorted = sorted(candidates, reverse=True)  # cao → thấp
                tp_near = candidates_sorted[0]
                tp_far = candidates_sorted[-1]
            else:
                tp_near = tp_far = None
        
            # ===== SL bảo vệ lời =====
            sl_near = max(cur + 0.45 * a, entry - 0.10 * a) if move_atr > 0.8 else entry + 0.30 * a
            sl_far = entry + 0.80 * a
        
            lines.append(f"- Side: SELL")
            lines.append(f"- TP gần: {_f(tp_near)}")
            lines.append(f"- TP xa: {_f(tp_far)}")
            lines.append(f"- SL bảo vệ lời gần: {_f(sl_near)}")
            lines.append(f"- SL bảo vệ lời xa: {_f(sl_far)}")
        
            if move_atr >= 1.2:
                lines.append("- Lệnh đang lời tốt → ưu tiên trailing theo high 3 nến M15.")
            elif move_atr >= 0.8:
                lines.append("- Lệnh đã đủ lợi thế → cân nhắc dời SL về BE / khóa một phần lời.")
            else:
                lines.append("- Lệnh chưa chạy đủ xa → chưa nên trailing quá sát.")
        
            lines.append("- Không add SELL nếu giá đã rơi xa; chỉ add khi break low + retest giữ dưới.")
        else:
            # ===== FIX TP ORDER =====
            candidates = [x for x in [near_resistance, tp1_s, tp2_s] if x is not None]
        
            if candidates:
                candidates_sorted = sorted(candidates)  # thấp → cao
                tp_near = candidates_sorted[0]
                tp_far = candidates_sorted[-1]
            else:
                tp_near = tp_far = None
        
            # ===== SL bảo vệ lời =====
            sl_near = min(cur - 0.45 * a, entry + 0.10 * a) if move_atr > 0.8 else entry - 0.30 * a
            sl_far = entry - 0.80 * a
        
            lines.append(f"- Side: BUY")
            lines.append(f"- TP gần: {_f(tp_near)}")
            lines.append(f"- TP xa: {_f(tp_far)}")
            lines.append(f"- SL bảo vệ lời gần: {_f(sl_near)}")
            lines.append(f"- SL bảo vệ lời xa: {_f(sl_far)}")
        
            if move_atr >= 1.2:
                lines.append("- Lệnh đang lời tốt → ưu tiên trailing theo low 3 nến M15.")
            elif move_atr >= 0.8:
                lines.append("- Lệnh đã đủ lợi thế → cân nhắc dời SL về BE / khóa một phần lời.")
            else:
                lines.append("- Lệnh chưa chạy đủ xa → chưa nên trailing quá sát.")
        
            lines.append("- Không add BUY nếu giá đã bay xa; chỉ add khi break high + retest giữ trên.")
    
    if session_v4 or htf_pressure_v4 or close_confirm_v4 or macro_v4 or playbook_v4:
        lines.append("")
        lines.append("🧩 Toàn Cảnh Thị Trường:")
        if session_v4.get("session_tag"):
            lines.append(f"- Session: {session_v4.get('session_tag')} | Follow-through: {session_v4.get('follow_through')} | Fake risk: {session_v4.get('fake_move_risk')}")
        if htf_pressure_v4.get("state"):
            lines.append(f"- HTF Pressure: {htf_pressure_v4.get('state')} | H1 close: {htf_pressure_v4.get('h1_close_bias')} | H4 close: {htf_pressure_v4.get('h4_close_bias')}")
        if close_confirm_v4.get("strength") not in (None, "N/A"):
            lines.append(f"- Close Confirm: {close_confirm_v4.get('strength')} | Break valid: {'YES' if close_confirm_v4.get('break_valid') else 'NO'} | Hold: {close_confirm_v4.get('hold')}")
        if macro_v4.get("headline"):
            lines.append(f"- Macro: {macro_v4.get('headline')} | Bias: {macro_v4.get('bias')} | {macro_v4.get('note')}")
        if playbook_v4.get("quality"):
            trig = ", ".join(playbook_v4.get("trigger_pack") or [])
            lines.append(f"- Playbook V4: quality={playbook_v4.get('quality')}" + (f" | triggers: {trig}" if trig else ""))

    # ===== PRO DESK REVIEW =====
    tw1 = meta.get("trap_warning_v1") or {}
    tg2 = meta.get("trigger_engine_v2") or {}
    me1 = meta.get("master_engine_v1") or {}

    try:
        rp = float(pos) if pos is not None else None
    except Exception:
        rp = None

    # position quality v2 (local, always aligned with gate)
    pos_internal = 0
    if final_score >= 55:
        pos_internal += 2
    elif final_score >= 35:
        pos_internal += 1

    if str(side).upper() == "SELL":
        if rp is not None and rp >= 0.65:
            pos_internal += 2
        elif rp is not None and rp <= 0.25:
            pos_internal -= 2
        if gate.get("lh"):
            pos_internal += 1
        if gate.get("break_dn"):
            pos_internal += 1
    else:
        if rp is not None and rp <= 0.35:
            pos_internal += 2
        elif rp is not None and rp >= 0.75:
            pos_internal -= 2
        if gate.get("hl"):
            pos_internal += 1
        if gate.get("break_up"):
            pos_internal += 1

    conflict_reasons = []
    if isinstance(liquidation, dict) and liquidation.get("ok"):
        conflict_reasons.append("vừa có liquidation")
    if isinstance(no_trade_zone, dict) and no_trade_zone.get("active"):
        conflict_reasons.extend([str(x) for x in (no_trade_zone.get("reasons") or []) if x][:2])
    if (tw1 or {}).get("active"):
        conflict_reasons.extend([str(x) for x in (tw1.get("warnings") or []) if x][:2])

    if str(side).upper() == "SELL" and not gate.get("lh"):
        conflict_reasons.append("SELL chưa có LH xác nhận")
    if str(side).upper() == "BUY" and not gate.get("hl"):
        conflict_reasons.append("BUY chưa có HL xác nhận")
    if str(side).upper() == "SELL" and rp is not None and rp <= 0.25:
        conflict_reasons.append("SELL đang ở vùng thấp")
    if str(side).upper() == "BUY" and rp is not None and rp >= 0.75:
        conflict_reasons.append("BUY đang ở vùng cao")

    # dedupe
    _conf = []
    for x in conflict_reasons:
        if x not in _conf:
            _conf.append(x)
    conflict_reasons = _conf[:4]
    conflict_sev = len(conflict_reasons)

    pos_internal -= 2 if conflict_sev >= 4 else (1 if conflict_sev >= 2 else 0)
    trigger_state = str((tg2 or {}).get("state") or "WAIT").upper()
    if trigger_state == "TRIGGERED":
        pos_internal += 1
    elif trigger_state == "WAIT":
        pos_internal -= 1

    if pos_internal >= 4:
        pq_quality = "STRONG"
        pq_reason = "vị trí tốt + cấu trúc hỗ trợ + conflict thấp"
    elif pos_internal >= 1:
        pq_quality = "MID"
        pq_reason = "có một phần lợi thế nhưng chưa đủ sạch để tăng rủi ro"
    else:
        pq_quality = "WEAK"
        pq_reason = "vị trí/cấu trúc chưa đủ đẹp hoặc conflict còn cao"

    # exit engine local
    if str(side).upper() == "SELL":
        structure_status = "CONFIRMED_SELL" if gate.get("lh") and gate.get("break_dn") else ("LH_ONLY" if gate.get("lh") else "NO_LH")
    else:
        structure_status = "CONFIRMED_BUY" if gate.get("hl") and gate.get("break_up") else ("HL_ONLY" if gate.get("hl") else "NO_HL")

    invalidation_hit = False
    try:
        if sl is not None and cur is not None:
            invalidation_hit = (float(cur) > float(sl)) if str(side).upper() == "SELL" else (float(cur) < float(sl))
    except Exception:
        invalidation_hit = False

    if invalidation_hit:
        ex_state = "EXIT_NOW"
        ex_decision = "Thoát ngay / cắt mạnh"
    elif str(side).upper() == "SELL":
        if gate.get("lh") and gate.get("break_dn") and conflict_sev < 2:
            ex_state = "HOLD"
            ex_decision = "Có thể giữ tiếp"
        elif gate.get("lh"):
            ex_state = "HOLD_LIGHT"
            ex_decision = "Giữ nhẹ, chưa add"
        elif conflict_sev >= 2:
            ex_state = "REDUCE_RISK"
            ex_decision = "Ưu tiên giảm size / giữ rất ngắn"
        else:
            ex_state = "CUT_SOON"
            ex_decision = "Không add, sẵn sàng thoát nếu không cải thiện"
    else:
        if gate.get("hl") and gate.get("break_up") and conflict_sev < 2:
            ex_state = "HOLD"
            ex_decision = "Có thể giữ tiếp"
        elif gate.get("hl"):
            ex_state = "HOLD_LIGHT"
            ex_decision = "Giữ nhẹ, chưa add"
        elif conflict_sev >= 2:
            ex_state = "REDUCE_RISK"
            ex_decision = "Ưu tiên giảm size / giữ rất ngắn"
        else:
            ex_state = "CUT_SOON"
            ex_decision = "Không add, sẵn sàng thoát nếu không cải thiện"

    risk_level = "HIGH" if invalidation_hit or conflict_sev >= 4 else ("MEDIUM" if conflict_sev >= 2 else "LOW")
    ex_reasons = []
    if str(side).upper() == "SELL" and not gate.get("lh"):
        ex_reasons.append("SELL chưa có LH")
    if str(side).upper() == "BUY" and not gate.get("hl"):
        ex_reasons.append("BUY chưa có HL")
    if conflict_sev >= 2:
        ex_reasons.append("conflict còn hiện diện")
    if trigger_state == "WAIT":
        ex_reasons.append("chưa có trigger hỗ trợ giữ lệnh")

    lines.append("")
    lines.append("🧠 ===== PRO DESK REVIEW =====")
    lines.append(f"📦 Position quality: {pq_quality}")
    lines.append(f"- {pq_reason}")
    lines.append("🎯 POSITION DECISION:")
    lines.append(f"- {ex_decision}")
    lines.append("🚪 EXIT ENGINE V2:")
    lines.append(f"- State: {ex_state}")
    lines.append(f"- Decision: {ex_decision}")
    lines.append(f"- Risk: {risk_level}")
    lines.append(f"- Structure: {structure_status}")
    lines.append(f"- Invalidation hit: {'YES' if invalidation_hit else 'NO'}")
    lines.append(f"- Add allowed: {'YES' if ex_state == 'HOLD' else 'NO'}")
    if ex_reasons:
        lines.append("- Lý do:")
        for s in ex_reasons[:4]:
            lines.append(f"  • {s}")

    if conflict_reasons:
        verdict = "HIGH CONFLICT" if conflict_sev >= 4 else ("MEDIUM CONFLICT" if conflict_sev >= 2 else "LOW CONFLICT")
        lines.append("⚖️ CONFLICT:")
        lines.append(f"- {verdict}")
        for s in conflict_reasons[:3]:
            lines.append(f"- {s}")

    lines.append("📌 REVIEW SUGGESTION:")
    if tradeable_label == "YES" and ex_state in ("HOLD", "HOLD_LIGHT"):
        lines.append("- HOLD / NO ADD")
        lines.append("- Giữ theo plan nhưng chưa nên mở thêm rủi ro mới.")
    else:
        lines.append("- NO TRADE")
        lines.append("- Chưa có lợi thế rõ để vào lệnh mới.")
        lines.append("- Ưu tiên đứng ngoài và chờ market lộ mặt thêm.")
    if isinstance(playbook, dict) and playbook.get("zone_low") is not None and playbook.get("zone_high") is not None:
        lines.append(f"- Chờ phản ứng tại vùng {_f(playbook.get('zone_low'))} – {_f(playbook.get('zone_high'))}")

    lines.append("🎯 REVIEW TRIGGER V2:")
    lines.append(f"- State: {trigger_state}")
    lines.append(f"- Quality: {str((tg2 or {}).get('quality') or 'LOW')}")
    for s in ((tg2 or {}).get("reason") or [])[:3]:
        lines.append(f"- {s}")

    if me1:
        lines.append("🧠 MASTER ENGINE:")
        lines.append(f"- State: {me1.get('state', 'WAIT')}")
        lines.append(f"- Best side: {me1.get('best_side', 'NONE')}")
        lines.append(f"- Tradeable final: {'YES' if me1.get('tradeable_final') else 'NO'}")
        lines.append(f"- Confidence: {me1.get('confidence', 'LOW')}")
        for s in (me1.get("reason") or [])[:3]:
            lines.append(f"- {s}")
    # ===== REVIEW DECISION ENGINE V2 =====
    try:
        rv2 = build_review_decision_engine_v2(
            side=side,
            pos=pos,
            gate=gate,
            ex_state=ex_state,
            trigger_state=trigger_state,
            entry=entry,
            cur=cur,
        )
    
        lines.append("")
        lines.append("🎯 QUYẾT ĐỊNH NHANH:")
        lines.append(f"→ {rv2.get('decision', 'WAIT')}")
        lines.append(f"📍 Entry quality: {rv2.get('entry_status', 'UNKNOWN')}")
        lines.append(f"➕ Add position: {rv2.get('add_action', '❌ Không add')}")
    
        if rv2.get("risk_note"):
            lines.append("⚠️ Risk:")
            for r in rv2["risk_note"]:
                lines.append(f"- {r}")
    
        if rv2.get("wait_conditions"):
            lines.append("⏳ Cần thêm điều kiện:")
            for w in rv2["wait_conditions"]:
                lines.append(f"- {w}")
    
        if rv2.get("wrong_reasons"):
            lines.append("❌ Nếu lệnh sai → vì:")
            for w in rv2["wrong_reasons"]:
                lines.append(f"- {w}")
    
    except Exception as e:
        lines.append("")
        lines.append(f"🎯 QUYẾT ĐỊNH NHANH: lỗi engine ({e})")
    # dedupe blank lines
    out = []
    for line in lines:
        if line == "" and (not out or out[-1] == ""):
            continue
        out.append(line)
    return "\n".join(out)


#def _fetch_triplet(symbol: str, limit: int = 260) -> Dict[str, List[Any]]:
    # M15, M30, H1
    #m15, _ = get_candles(symbol, "15min", limit)
    #m30, _ = get_candles(symbol, "30min", limit)
    #h1, _ = get_candles(symbol, "1h", limit)
    #return {"m15": m15, "m30": m30, "h1": h1}
def _fetch_triplet(symbol: str, limit: int = 260) -> Dict[str, Any]:
    # M15, M30, H1, H4 (H1+H4 confluence for Bias)
    sym = normalize_symbol(symbol)
    m15, src15 = _as_list_and_source_from_get_candles(get_candles(sym, "15min", limit=220))
    m30, src30 = _as_list_and_source_from_get_candles(get_candles(sym, "30min", limit=220))
    h1, src1h = _as_list_and_source_from_get_candles(get_candles(sym, "1h", limit=220))
    h4, src4h = _as_list_and_source_from_get_candles(get_candles(sym, "4h", limit=220))
    return {"m15": m15, "m30": m30, "h1": h1, "h4": h4, "data_source": (src30 or src15 or src1h or src4h)}

def _force_send(sig: dict) -> bool:
    ctx = " | ".join(sig.get("context_lines", []) or [])
    notes = " | ".join(sig.get("notes", []) or [])

    # Liquidity warning
    if "Liquidity WARNING" in ctx:
        return True

    # Post-sweep state
    if "POST-SWEEP" in ctx or "POST-SWEEP" in notes:
        return True

    return False

def _load_scale_state() -> dict:
    try:
        with open(SCALE_ALERT_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_scale_state(st: dict) -> None:
    tmp = f"{SCALE_ALERT_STATE_PATH}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    os.replace(tmp, SCALE_ALERT_STATE_PATH)
def _should_send_scale_alert(symbol: str, plan: dict) -> bool:
    """
    Gửi SCALE riêng mỗi 15' nhưng có cooldown + chống lặp trạng thái.
    Rule:
    - Stage 3: luôn đáng gửi
    - Stage 2: chỉ gửi khi readiness HIGH
    - Stage 1: không gửi
    """
    try:
        direction = str(plan.get("direction") or "").upper()
        stage_num = int(plan.get("stage_num") or 1)
        readiness = str(plan.get("readiness") or "").upper()

        if direction not in ("BUY", "SELL"):
            return False

        if stage_num == 1:
            return False

        if stage_num == 2 and readiness not in ("MEDIUM", "HIGH"):
            return False

        if stage_num == 3:
            pass
        elif stage_num == 2 and readiness == "HIGH":
            pass
        else:
            return False

        st = _load_scale_state()
        now_ts = int(time.time())
        key = f"{symbol}_SCALE"

        current_sig = {
            "direction": direction,
            "stage_num": stage_num,
            "readiness": readiness,
            "condition": str(plan.get("condition") or ""),
            "invalid": str(plan.get("invalid") or ""),
        }

        prev = st.get(key, {})
        last_ts = int(prev.get("ts", 0))
        prev_sig = prev.get("sig", {})

        # nếu y hệt trạng thái cũ và chưa hết cooldown -> không gửi
        if prev_sig == current_sig and (now_ts - last_ts) < SCALE_ALERT_COOLDOWN_MIN * 60:
            return False

        st[key] = {"ts": now_ts, "sig": current_sig}
        _save_scale_state(st)
        return True

    except Exception:
        return False
# =========================
# REGIME ALERT (CHOP / STOP-HUNT) - independent of stars
# =========================

REGIME_ALERT_STATE_PATH = os.getenv("REGIME_ALERT_STATE_PATH", "regime_alert_state.json")
REGIME_ALERT_COOLDOWN_MIN = int(os.getenv("REGIME_ALERT_COOLDOWN_MIN", "60"))  # 2h
REGIME_CHOP_THRESHOLD = float(os.getenv("REGIME_CHOP_THRESHOLD", "6.8"))        # 0..10
REGIME_ALERT_ENABLED = os.getenv("REGIME_ALERT_ENABLED", "1").strip() != "0"


def _load_json(path: str, default: dict):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json_atomic(path: str, data: dict):
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _wick_stats(o: float, h: float, l: float, c: float):
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l
    rng = max(h - l, 1e-9)
    wick_total = max(upper + lower, 0.0)
    wick_body = wick_total / max(body, 1e-9)   # wick/body (rất lớn => nhiễu)
    wick_rng = wick_total / rng                # wick/range (0.6+ => wick chiếm ưu thế)
    return wick_body, wick_rng


def _avg_wickiness(candles, bars: int):
    cs = _m15_closed(candles)  # bỏ nến đang chạy (nếu có)
    if not cs or len(cs) < bars:
        return None
    w = cs[-bars:]
    wb, wr = [], []
    for x in w:
        o = _cget(x, "open"); h = _cget(x, "high"); l = _cget(x, "low"); c = _cget(x, "close")
        a, b = _wick_stats(o, h, l, c)
        wb.append(a); wr.append(b)
    return (sum(wb) / len(wb), sum(wr) / len(wr))


def _netmove_pct(candles, bars: int):
    cs = _m15_closed(candles)
    if not cs or len(cs) < bars:
        return None
    w = cs[-bars:]
    net = abs(_cget(w[-1], "close") - _cget(w[0], "open"))
    hi = max(_cget(x, "high") for x in w)
    lo = min(_cget(x, "low") for x in w)
    rng = max(hi - lo, 1e-9)
    return net / rng  # càng nhỏ => đi nhiều nhưng không tiến => chop


def _count_false_breaks(candles, lookback: int, level_lookback: int, eps: float = 0.0):
    cs = _m15_closed(candles)
    if not cs or len(cs) < level_lookback + 2:
        return 0

    n = len(cs)
    start = max(1, n - lookback)
    fb = 0

    for i in range(start, n):
        window = cs[max(0, i - level_lookback):i]
        hi_lvl = max(_cget(x, "high") for x in window)
        lo_lvl = min(_cget(x, "low") for x in window)

        o = _cget(cs[i], "open")
        h = _cget(cs[i], "high")
        l = _cget(cs[i], "low")
        c = _cget(cs[i], "close")

        # break high rồi đóng lại dưới level => false break up
        if h > hi_lvl + eps and c < hi_lvl:
            fb += 1
        # break low rồi đóng lại trên level => false break down
        if l < lo_lvl - eps and c > lo_lvl:
            fb += 1

    return fb


def score_chop_regime(m15, h1, h2=None):
    """
    Score 0..10: càng cao => càng giống chop/stop-hunt.
    Dùng dữ liệu ~2-3h đầu phiên cũng bắt được.
    """
    details = {}

    fb15 = _count_false_breaks(m15, lookback=24, level_lookback=48)
    fb1h = _count_false_breaks(h1,  lookback=12, level_lookback=36)
    details["false_breaks_15m"] = fb15
    details["false_breaks_1h"] = fb1h

    w15 = _avg_wickiness(m15, bars=12)  # ~3h
    if w15:
        wick_body, wick_rng = w15
    else:
        wick_body, wick_rng = None, None
    details["wick_body_avg_15m"] = wick_body
    details["wick_range_avg_15m"] = wick_rng

    nm15 = _netmove_pct(m15, bars=12)
    nm1h = _netmove_pct(h1,  bars=6)
    details["netmove_pct_15m"] = nm15
    details["netmove_pct_1h"] = nm1h

    nm2h = None
    wr2h = None
    if h2:
        w2 = _avg_wickiness(h2, bars=6)
        wr2h = w2[1] if w2 else None
        nm2h = _netmove_pct(h2, bars=4)
    details["wick_range_avg_2h"] = wr2h
    details["netmove_pct_2h"] = nm2h

    score = 0.0

    # False breaks là dấu hiệu “quét 2 đầu”
    score += min(fb15, 6) * 0.8   # max ~4.8
    score += min(fb1h, 4) * 0.7   # max ~2.8

    # Wick dominance
    if wick_rng is not None:
        if wick_rng >= 0.75: score += 2.2
        elif wick_rng >= 0.65: score += 1.6
        elif wick_rng >= 0.55: score += 1.0

    # Net move nhỏ => chop
    if nm15 is not None:
        if nm15 <= 0.22: score += 1.8
        elif nm15 <= 0.30: score += 1.2
    if nm1h is not None:
        if nm1h <= 0.25: score += 1.2
        elif nm1h <= 0.35: score += 0.8

    # 2H (nếu có) tăng độ chắc
    if nm2h is not None and wr2h is not None:
        if nm2h <= 0.30 and wr2h >= 0.60:
            score += 0.8

    score = max(0.0, min(10.0, score))
    return score, details


def maybe_send_regime_alert(symbol: str, m15, h1, h2=None, chat_id: Optional[str] = None):
    """
    Gửi cảnh báo nếu score >= threshold.
    Không phụ thuộc MIN_STARS.
    Có cooldown để không spam.
    """
    if not REGIME_ALERT_ENABLED:
        return False, 0.0, {}

    score, details = score_chop_regime(m15, h1, h2)

    st = _load_json(REGIME_ALERT_STATE_PATH, default={})
    now = int(time.time())
    key = f"{symbol}_CHOP"
    last_ts = int(st.get(key, 0))

    should_send = (score >= REGIME_CHOP_THRESHOLD) and (now - last_ts >= REGIME_ALERT_COOLDOWN_MIN * 60)
    if not should_send:
        return False, score, details

    msg = (
        f"⚠️ REGIME ALERT: CHOP / STOP-HUNT ({symbol})\n"
        f"Score: {score:.1f}/10 (>= {REGIME_CHOP_THRESHOLD})\n"
        f"- FalseBreak 15m: {details.get('false_breaks_15m')}\n"
        f"- FalseBreak 1h : {details.get('false_breaks_1h')}\n"
        f"- Wick(range)15m: {details.get('wick_range_avg_15m')}\n"
        f"- NetMove% 15m  : {details.get('netmove_pct_15m')}\n"
        f"⛔ Gợi ý: NO TRADE / đợi displacement thật + follow-through / tránh đặt SL ngay sau swing gần."
    )

    _send_long_telegram(msg, chat_id=chat_id or ADMIN_CHAT_ID)
    st[key] = now
    _save_json_atomic(REGIME_ALERT_STATE_PATH, st)

    return True, score, details

def _ingest_mt5_payload(payload: Dict[str, Any]) -> None:
    """
    payload shape from bridge:
      { "symbol": "...", "tf": "M15"/"M30"/"H1" (or "15"/"30"/"1h"), "candles": [ {ts/open/high/low/close/volume}, ... ] }

    Your app.data_source.ingest_mt5_candles signature varies by version.
    We'll support both:
      ingest_mt5_candles(payload)
      ingest_mt5_candles(symbol, tf, candles)
    """
    try:
        # try old signature (payload)
        ingest_mt5_candles(payload)  # type: ignore
        return
    except TypeError:
        pass

    symbol = (payload or {}).get("symbol")
    tf = (payload or {}).get("tf")
    candles = (payload or {}).get("candles")
    if not symbol or not tf or not isinstance(candles, list):
        raise ValueError("Invalid MT5 payload: require symbol/tf/candles")

    ingest_mt5_candles(symbol, tf, candles)  # type: ignore


@app.get("/", response_class=PlainTextResponse)
def root():
    return "OK"


# Accept MT5 push bridge
@app.post("/data/mt5", response_class=PlainTextResponse)
async def data_mt5(token: str = "", request: Request = None):
    secret = os.getenv("MT5_PUSH_SECRET", "")
    if not secret or token != secret:
        raise HTTPException(status_code=403, detail="Forbidden")
    payload = await request.json()
    try:
        _ingest_mt5_payload(payload)
    except Exception as e:
        logger.exception("[MT5] ingest failed: %s", e)
        raise HTTPException(status_code=400, detail=str(e))
    return "OK"


# Telegram webhook handler
@app.post("/telegram/webhook", response_class=PlainTextResponse)
async def telegram_webhook(request: Request):
    try:
        update = await request.json()
    except Exception:
        return "OK"

    msg = update.get("message") or update.get("edited_message") or {}
    text = (msg.get("text") or "").strip()
    chat = msg.get("chat") or {}
    chat_id = str(chat.get("id") or "")

    if not text:
        return "OK"

    # ===== VIEW COMMAND =====
    try:
        raw_text = str(text or "").strip().upper()
        parts = raw_text.split()
    
        symbol_map = {
            "XAU": "XAU/USD",
            "GOLD": "XAU/USD",
            "BTC": "BTC/USD",
            "BITCOIN": "BTC/USD",
        }
    
        if len(parts) >= 2 and parts[1] == "VIEW":
            sym = symbol_map.get(parts[0])
    
            if not sym:
                _send_long_telegram("Không nhận ra symbol. Dùng: XAU VIEW hoặc BTC VIEW", chat_id=chat_id)
                return "OK"
    
            data = _fetch_triplet(sym, limit=260)
            current_price = _cget(data["m15"][-1], "close", None) if data.get("m15") else None
    
            sig = analyze_pro(
                sym,
                data.get("m15") or [],
                data.get("m30") or [],
                data.get("h1") or [],
                data.get("h4") or [],
                current_price=current_price,
            )
    
            if not isinstance(sig, dict):
                _send_long_telegram(f"❌ VIEW lỗi: analyze_pro không trả dict cho {sym}", chat_id=chat_id)
                return "OK"
    
            ds = data.get("data_source")
            if ds:
                sig["data_source"] = ds
                sig.setdefault("meta", {})["data_source"] = ds
    
            view_text = build_view_engine_v1(sig)
            _send_long_telegram(view_text, chat_id=chat_id)
    
            return "OK"
    
    except Exception as e:
        logger.exception("VIEW COMMAND ERROR")
        _send_long_telegram(f"❌ VIEW failed: {e}", chat_id=chat_id)
        return "OK"
    # ===== SCALE V2 COMMAND =====
    if _is_scale_command(text):
        symbol = _parse_symbol_from_text(text)

        try:
            m15, src15 = _as_list_and_source_from_get_candles(get_candles(symbol, "15min", limit=260))
            m30, src30 = _as_list_and_source_from_get_candles(get_candles(symbol, "30min", limit=260))
            h1, src1h = _as_list_and_source_from_get_candles(get_candles(symbol, "1h", limit=260))
            h4, src4h = _as_list_and_source_from_get_candles(get_candles(symbol, "4h", limit=260))

            plan = build_scale_plan_v2(
                symbol=symbol,
                m15=m15,
                m30=m30,
                h1=h1,
                h4=h4,
                total_tp_cent=500.0,
                lot1=0.30,
                lot2=0.30,
                lot3=0.50,
            )

            ds = src30 or src15 or src1h or src4h
            msg = format_scale_plan_v2(plan)
            if ds:
                msg = msg.replace(f"📌 {symbol} SCALE | M15/H1", f"📌 {symbol} SCALE | M15/H1\n📡 Dữ liệu: {ds}")

            _send_long_telegram(msg, chat_id=chat_id)
            return "OK"

        except Exception as e:
            logger.exception("SCALE V2 failed for %s: %s", symbol, e)
            _send_long_telegram(f"❌ SCALE lỗi cho {symbol}: {e}", chat_id=chat_id)
            return "ERROR"
    # 0) ƯU TIÊN: Manual trade review (không cần "now")
    parsed = parse_manual_trade(text)
    if parsed:
        logger.info("[TG][REVIEW] matched manual trade: text=%r parsed=%s", text, parsed)
        try:
            reply = review_manual_trade(**parsed)
            if not reply:
                reply = "❌ REVIEW lỗi: hàm review không trả nội dung."
        except Exception as e:
            logger.exception("manual review failed: text=%r err=%s", text, e)
            reply = f"❌ REVIEW lỗi: {e}"
    
        try:
            _send_long_telegram(reply, chat_id=chat_id)
        except Exception as send_err:
            logger.exception(
                "send long telegram failed for manual review: chat_id=%s err=%s",
                chat_id, send_err
            )
            try:
                _send_long_telegram("❌ REVIEW lỗi: không gửi được tin nhắn dài. Xem logs.", chat_id=chat_id)
            except Exception:
                pass
    
        return "OK"

    low = text.lower()


    # 1) "now/scan" -> mới chạy analyze_pro
    if "now" in low or "scan" in low or "all" in low:
        if "scan" in low or "all" in low:
            symbols = DEFAULT_SYMBOLS or ["XAU/USD", "BTC/USD", "XAG/USD"]
        else:
            symbols = [_parse_symbol_from_text(text)]

        for sym in symbols:
            try:
                data = _fetch_triplet(sym, limit=260)
                # (optional) lấy thêm 2H cho regime radar (nếu get_candles hỗ trợ)
                try:
                    h2 = _as_list_from_get_candles(get_candles(sym, "2h", limit=220))
                except Exception:
                    h2 = []
                
                # ✅ Regime alert: independent of stars
                maybe_send_regime_alert(sym, data["m15"], data["h1"], h2=h2, chat_id=ADMIN_CHAT_ID)
                session = ""
                current_price = _cget(data["m15"][-1], "close", None) if data.get("m15") else None
                sig = analyze_pro(
                    sym,
                    data["m15"],
                    data["m30"],
                    data["h1"],
                    data["h4"],
                    current_price=current_price,
                )
                # --- Guard: analyze_pro phải trả dict, nếu không thì fallback để khỏi crash
                if not isinstance(sig, dict):
                    sig = {
                        "symbol": sym,
                        "tf": "M30",
                        "session": session,
                        "recommendation": "CHỜ",
                        "stars": 1,
                        "trade_mode": "MANUAL",
                        "meta": {
                            "data_source": data.get("data_source") if isinstance(data, dict) else None
                        },
                        "context_lines": ["- Context: n/a"],
                        "liquidity_lines": ["- n/a"],
                        "quality_lines": ["- n/a"],
                        "note_lines": ["⚠️ analyze_pro returned None → fallback signal"],
                    }
                # attach data source for Telegram
                try:
                    ds = data.get("data_source")
                    if ds:
                        sig["data_source"] = ds
                        sig.setdefault("meta", {})["data_source"] = ds
                except Exception:
                    pass

                stars = int(sig.get("stars", 0) or 0)
                force_send = _force_send(sig)

                if force_send:
                    prefix = "🚨 CẢNH BÁO THANH KHOẢN / POST-SWEEP\\n\\n"
                    _send_long_telegram(prefix + format_signal(sig), chat_id=chat_id)
                elif stars < MIN_STARS:
                    # Manual 'NOW/SCAN': always send full analysis, but hide trade plan when under the star gate
                    #sig["show_trade_plan"] = False
                    prefix = f"⚠️ (Manual) Kèo dưới {MIN_STARS}⭐ – tham khảo thôi.\n\n"
                    _send_long_telegram(prefix + format_signal(sig), chat_id=chat_id)
                else:
                    _send_long_telegram(format_signal(sig), chat_id=chat_id)

            except Exception as e:
                logger.exception("analysis failed: %s", e)
                _send_telegram(f"❌ Analysis failed ({sym}): {e}", chat_id=chat_id)

    return "OK"



# Cron endpoint
@app.get("/cron/run", response_class=PlainTextResponse)
@app.head("/cron/run", response_class=PlainTextResponse)
async def cron_run(token: str = "", request: Request = None):
    global LAST_CRON_TS

    if CRON_SECRET and token != CRON_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")

    now = int(time.time())

    # cooldown: return 200 to avoid external retries storm
    if now - LAST_CRON_TS < MIN_CRON_GAP_SEC:
        return "OK cooldown"

    if CRON_LOCK.locked():
        return "OK overlap"

    async with CRON_LOCK:
        LAST_CRON_TS = int(time.time())
        client = getattr(getattr(request, "client", None), "host", "unknown") if request else "unknown"
        logger.info("[CRON] start from=%s", client)

        symbols = DEFAULT_SYMBOLS or ["XAU/USD", "BTC/USD", "XAG/USD"]

        for sym in symbols:
            try:
                data = _fetch_triplet(sym, limit=260)
                session = ""
                current_price = _cget(data["m15"][-1], "close", None) if data.get("m15") else None
                sig = analyze_pro(
                    sym,
                    data["m15"],
                    data["m30"],
                    data["h1"],
                    data["h4"],
                    current_price=current_price,
                )
                # --- Guard: analyze_pro phải trả dict, nếu không thì fallback để khỏi crash
                if not isinstance(sig, dict):
                    sig = {
                        "symbol": sym,
                        "tf": "M30",
                        "session": session,
                        "recommendation": "CHỜ",
                        "stars": 1,
                        "trade_mode": "MANUAL",
                        "meta": {
                            "data_source": data.get("data_source") if isinstance(data, dict) else None
                        },
                        "context_lines": ["- Context: n/a"],
                        "liquidity_lines": ["- n/a"],
                        "quality_lines": ["- n/a"],
                        "note_lines": ["⚠️ analyze_pro returned None → fallback signal"],
                    }
                # attach data source for Telegram
                try:
                    ds = data.get("data_source")
                    if ds:
                        sig["data_source"] = ds
                        sig.setdefault("meta", {})["data_source"] = ds
                except Exception:
                    pass


                # ===== SCALE ALERT (separate from NOW) =====
                scale_plan = None
                should_send_scale = False
                try:
                    scale_plan = build_scale_plan_v2(
                        symbol=sym,
                        m15=data["m15"],
                        m30=data["m30"],
                        h1=data["h1"],
                        h4=data["h4"],
                        total_tp_cent=500.0,
                        lot1=0.30,
                        lot2=0.30,
                        lot3=0.50,
                    )

                    if ds:
                        scale_plan["data_source"] = ds

                    should_send_scale = _should_send_scale_alert(sym, scale_plan)

                except Exception as e:
                    logger.exception("[CRON] %s: SCALE build failed: %s", sym, e)
                    scale_plan = None
                    should_send_scale = False                    
                stars = int(sig.get("stars", 0) or 0)
                short_hint = sig.get("short_hint") or []
                entry = sig.get("entry")
                sl = sig.get("sl")
                tp1 = sig.get("tp1")
                rec = sig.get("recommendation", "")
                now_status = get_now_status(sig)
                setup_score = int(now_status.get("setup_score", 0) or 0)
                entry_score = int(now_status.get("entry_score_now", 0) or 0)
                tradeable_now = str(now_status.get("tradeable_now") or "NO")
                #force_send = _force_send(sig)                
                # ----- LUỒNG A: KÈO CHÍNH -----
                #should_send_main = stars >= MIN_STARS and rec != "CHỜ"
                #should_send_now = setup_score >= 65 and entry_score >= 50 and tradeable_now == "YES"
                #if should_send_main or force_send or should_send_now:
                try:
                    cls, score, _ = _setup_class_score_v3(sig)
                    score = float(score or 0)
                except Exception:
                    cls, score = "D", 0
                
                should_send_now = cls in ("A", "B") and score >= 50
                if should_send_now:
                    msg = format_signal(sig)
                    msg = "🚨 **NOW ALERT**\n----------------\n" + msg
                    _send_long_telegram(msg, chat_id=ADMIN_CHAT_ID)
                #if should_send_main or force_send or should_send_now:
                    #_send_telegram(format_signal(sig), chat_id=ADMIN_CHAT_ID)
                else:
                    logger.info("[CRON] %s: no telegram send | setup=%s entry=%s", sym, setup_score, entry_score)

                # ===== SEND SCALE ALERT SEPARATELY =====
                if should_send_scale and scale_plan:
                    try:
                        scale_msg = format_scale_plan_v2(scale_plan)
                
                        if ds:
                            scale_msg = scale_msg.replace(
                                f"📌 {sym} SCALE | M15/H1",
                                f"📌 {sym} SCALE | M15/H1\n📡 Dữ liệu: {ds}"
                            )
                
                        scale_msg = "🚀 SCALE ALERT\n━━━━━━━━━━━━━━\n" + scale_msg
                
                        logger.info(
                            "[CRON][SCALE ALERT] %s: SEND | stage=%s readiness=%s direction=%s",
                            sym,
                            scale_plan.get("stage_num", "n/a"),
                            scale_plan.get("readiness", "n/a"),
                            scale_plan.get("direction", "n/a"),
                        )
                
                        _send_long_telegram(scale_msg, chat_id=ADMIN_CHAT_ID)
                
                    except Exception as e:
                        logger.exception("[CRON][SCALE SEND] %s failed: %s", sym, e)
                else:
                    if scale_plan:
                        logger.info(
                            "[CRON][SCALE] %s: skip send | stage=%s readiness=%s direction=%s",
                            sym,
                            scale_plan.get("stage_num", "n/a"),
                            scale_plan.get("readiness", "n/a"),
                            scale_plan.get("direction", "n/a"),
                        )
            except Exception as e:
                logger.exception("[CRON] %s failed: %s", sym, e)

        return "OK"
