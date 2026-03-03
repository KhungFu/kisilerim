# -*- coding: utf-8 -*-
# NEXUS CEO v10.1 - Freie Chat-Interaktion + Spread-Schreiber + Bugfixes
# Spread Filter: Max 0.5 | Weekend Crypto: AKTIF | Heartbeat: AKTIF
import os, time, requests, telebot, re, logging, json, threading, sys
from google import genai
from google.genai import types
from datetime import datetime
from dotenv import load_dotenv

# --- CONFIG IMPORT VERSUCH ---
try:
    from capital_markets_config import MARKET_CONFIG
    logging.info("✅ Externe capital_markets_config.py geladen!")
except ImportError:
    logging.warning("⚠️ Keine capital_markets_config.py gefunden. Nutze interne Fallback-Liste.")
    MARKET_CONFIG = {
        "EURUSD": {"epic": "EURUSD", "min_size": 100.0},
        "XRP_USD": {"epic": "XRPUSD", "min_size": 1.0},
        "SOL_USD": {"epic": "SOLUSD", "min_size": 0.1},
        "GOLD": {"epic": "GOLD", "min_size": 0.01},
        "BTC_USD": {"epic": "BTCUSD", "min_size": 0.01},
    }

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

# ============================================================
# KONFIGURASYON
# ============================================================
TG_TOKEN = os.getenv("TG_TOKEN") or os.getenv("TELEGRAM_TOKEN")
MY_CHAT_ID = os.getenv("MY_CHAT_ID")
GEMINI_KEYS = [os.getenv(f"GEMINI_API_KEY_{i}") for i in range(1, 7)]
CAP_KEY = os.getenv("CAPITAL_API_KEY")
CAP_ID = os.getenv("CAPITAL_IDENTIFIER")
CAP_PW = os.getenv("CAPITAL_PASSWORD")
CAPITAL_URL = os.getenv("CAPITAL_URL") or "https://demo-api-capital.backend-capital.com/api/v1"

MAX_SPREAD = 0.5

# ============================================================
# GEMINI MODELLER UND ROTASYON
# ============================================================
GEMINI_MODELS = [
    "gemini-3-flash-preview",           # ÖNCELİKLİ
    "gemini-2.5-flash-preview-04-17",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemini-1.5-flash",
    "gemini-1.5-flash-8b",
]

_model_lock = threading.Lock()
_current_model_idx = 0

def get_next_model():
    global _current_model_idx
    with _model_lock:
        return GEMINI_MODELS[_current_model_idx % len(GEMINI_MODELS)]

def rotate_model_on_quota():
    global _current_model_idx
    with _model_lock:
        _current_model_idx = (_current_model_idx + 1) % len(GEMINI_MODELS)
        return GEMINI_MODELS[_current_model_idx % len(GEMINI_MODELS)]

bot = telebot.TeleBot(TG_TOKEN)

# ============================================================
# PYRAMIDING TAKIP - JSON-Datei (bleibt nach Neustart erhalten)
# ============================================================
PYRAMIDING_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pyramiding_state.json")
pyramiding_lock = threading.Lock()

def _load_pyramiding():
    try:
        if os.path.exists(PYRAMIDING_FILE):
            with open(PYRAMIDING_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logging.warning(f"Pyramiding-Datei Lesefehler: {e}")
    return {}

def _save_pyramiding(data):
    try:
        with open(PYRAMIDING_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logging.error(f"Pyramiding-Datei Schreibfehler: {e}")

def get_pyramiding_stufe(epic):
    with pyramiding_lock:
        return _load_pyramiding().get(epic, 0)

def set_pyramiding_stufe(epic, stufe):
    with pyramiding_lock:
        data = _load_pyramiding()
        data[epic] = stufe
        _save_pyramiding(data)

def reset_pyramiding_stufe(epic):
    with pyramiding_lock:
        data = _load_pyramiding()
        if epic in data:
            del data[epic]
        _save_pyramiding(data)

def sync_pyramiding_from_capital():
    try:
        h = capital_session.get_headers()
    except Exception as e:
        return f"Pyramiding-Sync: Session nicht bereit ({e})"
    if not h: return "Pyramiding-Sync: Keine API-Verbindung"
    try:
        pozisyonlar = get_positions(h)
    except Exception as e:
        return f"Pyramiding-Sync Fehler: {e}"
    epic_count = {}
    for p in pozisyonlar:
        epic = p['market']['epic']
        epic_count[epic] = epic_count.get(epic, 0) + 1
    with pyramiding_lock:
        alte_daten = _load_pyramiding()
        korrekturen = []
        for epic, anzahl in epic_count.items():
            if alte_daten.get(epic, 0) != anzahl:
                korrekturen.append(f"  {epic}: {alte_daten.get(epic,0)} -> {anzahl}")
        for epic in alte_daten:
            if epic not in epic_count:
                korrekturen.append(f"  {epic}: {alte_daten[epic]} -> 0 (geschlossen)")
        _save_pyramiding(epic_count)
    if korrekturen:
        return "Pyramiding-Sync korrigiert:\n" + "\n".join(korrekturen)
    return f"Pyramiding-Sync: {len(epic_count)} Epics korrekt"

# ============================================================
# CAPITAL.COM HELPERS (SESSION CACHING)
# ============================================================
class CapitalSession:
    def __init__(self):
        self.cst = None
        self.token = None
        self.expires = 0
        self.lock = threading.Lock()

    def get_headers(self):
        with self.lock:
            if time.time() < self.expires and self.cst:
                return {
                    "X-CAP-API-KEY": CAP_KEY,
                    "CST": self.cst,
                    "X-SECURITY-TOKEN": self.token,
                    "Content-Type": "application/json"
                }
            try:
                r = requests.post(
                    f"{CAPITAL_URL}/session",
                    json={"identifier": CAP_ID, "password": CAP_PW},
                    headers={"X-CAP-API-KEY": CAP_KEY},
                    timeout=15
                )
                if r.status_code == 200:
                    self.cst = r.headers.get("CST")
                    self.token = r.headers.get("X-SECURITY-TOKEN")
                    self.expires = time.time() + 1200
                    logging.info("✅ Neue Session erstellt")
                    return {
                        "X-CAP-API-KEY": CAP_KEY,
                        "CST": self.cst,
                        "X-SECURITY-TOKEN": self.token,
                        "Content-Type": "application/json"
                    }
            except Exception as e:
                logging.error(f"Session hatasi: {e}")
            return None

capital_session = CapitalSession()

def get_positions(h):
    try:
        return requests.get(f"{CAPITAL_URL}/positions", headers=h, timeout=10).json().get('positions', [])
    except:
        return []

def get_account_info(h):
    try:
        acc_req = requests.get(f"{CAPITAL_URL}/accounts", headers=h, timeout=10).json()
        if not acc_req.get('accounts'):
            logging.error("Hesap listesi bos - Capital.com session hatasi")
            return None
        acc = acc_req['accounts'][0]
        return {
            "nakit": acc['balance'].get('balance', 0),
            "toplam": acc['balance'].get('deposit', 0),
            "upl": acc['balance'].get('profitLoss', 0),
            "marjin": acc['balance'].get('balance', 0) - acc['balance'].get('available', 0),
            "musait": acc['balance'].get('available', 0)
        }
    except:
        return {"nakit": 0, "toplam": 0, "upl": 0, "marjin": 0, "musait": 0}

# ============================================================
# INDIKATOREN (ADX, RSI, MA)
# ============================================================
def berechne_adx(highs, lows, closes, period=14):
    if len(closes) < period + 1: return 0
    tr_list, plus_dm, minus_dm = [], [], []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        tr_list.append(tr)
        up = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        plus_dm.append(up if up > down and up > 0 else 0)
        minus_dm.append(down if down > up and down > 0 else 0)
    avg_tr = sum(tr_list[-period:]) / period
    avg_plus = sum(plus_dm[-period:]) / period
    avg_minus = sum(minus_dm[-period:]) / period
    if avg_tr == 0: return 0
    plus_di = (avg_plus / avg_tr) * 100
    minus_di = (avg_minus / avg_tr) * 100
    if (plus_di + minus_di) == 0: return 0
    dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100
    return dx

def berechne_rsi(prices, period=14):
    if len(prices) < period + 1: return 50
    gains, losses = 0, 0
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        if diff > 0: gains += diff
        else: losses -= diff
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0: return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def hesapla_ma(prices, period):
    if len(prices) < period: return None
    return sum(prices[-period:]) / period

def get_candles(epic, resolution, max_candles=30):
    h = capital_session.get_headers()
    if not h: return {'close': [], 'high': [], 'low': []}
    try:
        url = f"{CAPITAL_URL}/prices/{epic}?resolution={resolution}&max={max_candles}"
        r = requests.get(url, headers=h, timeout=15)
        if r.status_code == 200:
            prices_data = r.json().get('prices', [])
            closes, highs, lows = [], [], []
            for p in prices_data:
                c = p.get('closePrice', {}).get('bid', None)
                h_val = p.get('highPrice', {}).get('bid', None)  # FIX: highPrice statt high
                l_val = p.get('lowPrice', {}).get('bid', None)   # FIX: lowPrice statt low
                if c is not None: closes.append(float(c))
                if h_val is not None: highs.append(float(h_val))
                if l_val is not None: lows.append(float(l_val))
            return {'close': closes, 'high': highs, 'low': lows}
    except Exception as e:
        logging.warning(f"Mum verisi alinamadi {epic}/{resolution}: {e}")
    return {'close': [], 'high': [], 'low': []}

# ============================================================
# 2-of-3 TEKNİK KONTROL (MA + ADX + RSI) - BUGFIX
# ============================================================
def technical_confluence(epic):
    data = get_candles(epic, "HOUR_4", 30)
    closes = data.get('close', [])
    highs = data.get('high', [])
    lows = data.get('low', [])

    # BUGFIX: Sicherstellen dass alle Listen lang genug sind
    min_len = min(len(closes), len(highs), len(lows))
    if min_len < 26:
        return "NOTR", 0, f"Veri yetersiz ({min_len} mum)"

    closes = closes[-min_len:]
    highs = highs[-min_len:]
    lows = lows[-min_len:]

    ma9 = hesapla_ma(closes, 9)
    ma26 = hesapla_ma(closes, 26)
    if ma9 is None or ma26 is None: return "NOTR", 0, "MA hesaplanamadı"
    ma_signal = "BUY" if ma9 > ma26 else "SELL" if ma9 < ma26 else "NOTR"

    adx = berechne_adx(highs, lows, closes)
    adx_ok = adx > 20

    rsi = berechne_rsi(closes)
    rsi_ok = False
    if ma_signal == "BUY" and rsi < 70: rsi_ok = True
    if ma_signal == "SELL" and rsi > 30: rsi_ok = True

    score = 0
    if ma_signal != "NOTR": score += 1
    if adx_ok: score += 1
    if rsi_ok: score += 1

    details = f"MA:{ma_signal} ADX:{adx:.1f} RSI:{rsi:.1f}"
    if score >= 2: return ma_signal, score, details
    else: return "NOTR", score, details

# ============================================================
# SPREAD IN capital_markets_config.py SCHREIBEN
# ============================================================
def update_spreads_in_config(spread_data: dict):
    """
    Schreibt aktuelle Spreads in capital_markets_config.py.
    spread_data = {"EURUSD": 0.00012, "BTCUSD": 15.3, ...}
    """
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "capital_markets_config.py")
    if not os.path.exists(config_path):
        logging.warning("capital_markets_config.py nicht gefunden - Spread-Update übersprungen")
        return

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read()

        for symbol, spread in spread_data.items():
            # Suche nach "SYMBOL": { ... } und füge/ersetze "spread": X ein
            # Pattern: Eintrag für dieses Symbol finden
            pattern = rf'("{symbol}"\s*:\s*\{{[^}}]*?)(\}})'
            def replacer(m, sym=symbol, sp=spread):
                block = m.group(1)
                closing = m.group(2)
                if '"spread"' in block:
                    # Ersetze bestehenden spread-Wert
                    block = re.sub(r'"spread"\s*:\s*[\d\.]+', f'"spread": {sp:.6f}', block)
                else:
                    # Füge spread am Ende des Blocks hinzu
                    block = block.rstrip() + f',\n        "spread": {sp:.6f}\n    '
                return block + closing

            new_content = re.sub(pattern, replacer, content, flags=re.DOTALL)
            content = new_content

        with open(config_path, "w", encoding="utf-8") as f:
            f.write(content)

        logging.info(f"✅ Spreads in capital_markets_config.py aktualisiert ({len(spread_data)} Assets)")
    except Exception as e:
        logging.error(f"Spread-Schreibfehler: {e}")


def scan_and_write_spreads():
    """Liest aktuelle Spreads von Capital.com und schreibt sie in die Config."""
    h = capital_session.get_headers()
    if not h:
        return {}

    spread_data = {}
    for sym, cfg in MARKET_CONFIG.items():
        try:
            epic = cfg["epic"]
            r = requests.get(f"{CAPITAL_URL}/markets/{epic}", headers=h, timeout=10)
            if r.status_code == 200:
                snapshot = r.json().get('snapshot', {})
                bid = snapshot.get('bid', 0)
                offer = snapshot.get('offer', 0)
                if bid and offer:
                    spread = round(abs(offer - bid), 6)
                    spread_data[sym] = spread
        except Exception as e:
            logging.warning(f"Spread-Scan Fehler {sym}: {e}")

    if spread_data:
        update_spreads_in_config(spread_data)

    return spread_data

# ============================================================
# SPREAD & WEEKEND KONTROL
# ============================================================
def check_spread_ok(epic, spread):
    if spread > MAX_SPREAD: return False, f"Spread çok yüksek: {spread}"
    return True, "OK"

def is_weekend():
    return datetime.now().weekday() >= 5

def is_crypto(sym_key):
    # Prueft Config-Key UND epic - z.B. "ETH_USD" oder "ETHUSD"
    crypto_keywords = [
        "XRP", "SOL", "ADA", "LTC", "BTC", "ETH", "DOT", "AVAX",
        "LINK", "UNI", "AAVE", "ATOM", "ALGO", "VET", "HBAR",
        "IOTA", "EOS", "TRX", "XTZ", "XLM", "MATIC",
        "POL",
    ]
    # Pruefe auch den epic aus der Config falls vorhanden
    epics_to_check = [sym_key]
    if sym_key in MARKET_CONFIG:
        epics_to_check.append(MARKET_CONFIG[sym_key].get("epic", ""))
    return any(k in s.upper() for k in crypto_keywords for s in epics_to_check)

def check_weekend_allowed(sym_key):
    if is_weekend():
        if not is_crypto(sym_key): return False, "Haftasonu: Sadece kripto!"
        return True, "Kripto haftasonu acik"
    return True, "Hafta ici"

# ============================================================
# VOLATILITE KORUMA (KARA KUĞU)
# ============================================================
def volatilite_kontrol(h):
    pozisyonlar = get_positions(h)
    kapatilanlar = []
    for p in pozisyonlar:
        try:
            epic = p['market']['epic']
            upl = float(p['position']['upl'])
            level = float(p['position']['level'])
            current_bid = float(p['market'].get('bid', level))
            direction = p['position']['direction']
            deal_id = p['position']['dealId']
            instrument = p['market']['instrumentName']
            if level > 0:
                if direction == "BUY": degisim_pct = ((current_bid - level) / level) * 100
                else: degisim_pct = ((level - current_bid) / level) * 100
                if degisim_pct <= -10:
                    logging.warning(f"KARA KUĞU: {instrument} {degisim_pct:.1f}% - Kapatiliyor!")
                    r = requests.delete(f"{CAPITAL_URL}/positions/{deal_id}", headers=h, timeout=10)
                    if r.status_code == 200:
                        kapatilanlar.append(f"🚨 {instrument}: {degisim_pct:.1f}% kaybetti - KAPATILDI")
                        reset_pyramiding_stufe(epic)
        except Exception as e:
            logging.error(f"Volatilite kontrol hatasi: {e}")
    if kapatilanlar:
        alarm_mesaj = "⚠️ KARA KUĞU ALARMI!\n" + "\n".join(kapatilanlar)
        try: bot.send_message(MY_CHAT_ID, alarm_mesaj)
        except: pass
    return kapatilanlar

# ============================================================
# PYRAMIDING & DOKTRIN
# ============================================================
def pyramiding_kontrol(h, epic, instrument):
    stufe = get_pyramiding_stufe(epic)
    if stufe >= 4: return False, f"{instrument} maks 4 pyramiding seviyesine ulasti"
    pozisyonlar = get_positions(h)
    epic_pozisyonlar = [p for p in pozisyonlar if p['market']['epic'] == epic]
    if not epic_pozisyonlar: return True, "Ilk giris"
    for p in epic_pozisyonlar:
        upl = float(p['position']['upl'])
        level = float(p['position']['level'])
        size = float(p['position']['size'])
        if level > 0 and size > 0:
            maliyet = level * size
            if maliyet > 0:
                kar_pct = (upl / maliyet) * 100
                if kar_pct >= 2.0: return True, f"Pozisyon %{kar_pct:.1f} karda - Pyramiding izinli"
                else: return False, f"Pozisyon sadece %{kar_pct:.1f} karda - Min %2 gerekli"
    return False, "Pyramiding icin yeterli kar yok"

def load_doctrine():
    try:
        with open("https://github.com/KhungFu/kisilerim/blob/main/mentor_name.txt", "r", encoding="utf-8") as f: return f.read()
    except: return "Özel doktrin yok. Standart kurallar uygulanır."

# ============================================================
# GREMIUM OYLAMA
# ============================================================
def gremium_oylama(sinyal, guc, sym_key, instrument, upl_toplam, saat):
    oylar = {}
    is_krypto = is_crypto(sym_key)
    is_gece = 23 <= saat or saat < 6
    is_haftasonu = is_weekend()

    if sinyal in ["BUY", "SELL"] and guc >= 2: oylar["Cihat"] = "JA"
    else: oylar["Cihat"] = "NEIN"

    if guc >= 2 and not (is_krypto and is_gece and guc < 3): oylar["Dalio"] = "JA"
    else: oylar["Dalio"] = "NEIN"

    oylar["Kiyosaki"] = "JA" if sinyal != "NOTR" else "NEIN"
    oylar["Graham"] = "JA" if guc == 3 else "NEIN"
    oylar["Buffett"] = "JA" if guc >= 2 else "NEIN"

    if upl_toplam < -30: oylar["Sander"] = "NEIN"
    else: oylar["Sander"] = "JA" if sinyal != "NOTR" else "NEIN"

    if is_krypto and is_gece: oylar["Kostolany"] = "NEIN"
    else: oylar["Kostolany"] = "JA" if guc >= 2 else "NEIN"

    oylar["Lynch"] = "JA" if sinyal != "NOTR" else "NEIN"
    if guc < 2 or (is_krypto and is_gece): oylar["Taleb"] = "NEIN"
    else: oylar["Taleb"] = "JA"
    oylar["Munger"] = "JA" if guc >= 2 else "NEIN"
    oylar["Druckenmiller"] = "JA" if guc == 3 else ("NEIN" if guc < 2 else "JA")

    ja_sayisi = sum(1 for v in oylar.values() if v == "JA")
    if is_haftasonu and is_krypto: karar = ja_sayisi >= 5
    else: karar = ja_sayisi >= 6
    return karar, ja_sayisi, len(oylar) - ja_sayisi, oylar

# ============================================================
# GEMINI ANALIZ (AUTONOMOUS)
# ============================================================
def fetch_strategic_response(prompt_type="AUTONOMOUS", extra_data=None):
    h = capital_session.get_headers()
    if not h: return "API Baglanti Hatasi"
    acc = get_account_info(h)
    pozisyonlar = get_positions(h)
    portfolio = []
    for p in pozisyonlar:
        stufe = get_pyramiding_stufe(p['market']['epic'])
        portfolio.append({
            "asset": p['market']['instrumentName'], "epic": p['market']['epic'],
            "upl": p['position']['upl'], "size": p['position']['size'],
            "dir": p['position']['direction'], "level": p['position'].get('level', 0),
            "pyramiding_stufe": stufe
        })

    tech_sinyaller = {}
    count = 0
    for k, v in MARKET_CONFIG.items():
        if is_weekend() and not is_crypto(k):
            continue
        if not is_weekend() and count >= 15:
            break
        sinyal, guc, aciklama = technical_confluence(v['epic'])
        tech_sinyaller[k] = {"sinyal": sinyal, "guc": guc, "aciklama": aciklama}
        count += 1

    market_intel = {}
    for k, v in MARKET_CONFIG.items():
        # FIX3
        if is_weekend() and not is_crypto(k):
            continue
        try:
            p_res = requests.get(f"{CAPITAL_URL}/markets/{v['epic']}", headers=h, timeout=10).json()
            snapshot = p_res.get('snapshot', {})
            bid = snapshot.get('bid', 0)
            offer = snapshot.get('offer', 0)
            spread = round(abs(offer - bid), 5) if offer and bid else 999
            market_intel[k] = {"price": bid, "spread": spread}
        except: market_intel[k] = {"price": 0, "spread": 999}

    current_model = get_next_model()
    saat = datetime.now().hour
    dynamic_doctrine = load_doctrine()

    # Gemini bekommt die exakte Symbol-Liste damit er keine Namen erfindet
    symbol_liste = "\n".join([f"  {k}" for k in MARKET_CONFIG.keys()])

    system_prompt = f"""Sen NEXUS CEO v10.1 - Hibrit Gremium modundasın.
Konuşma tarzı: Cihat E. Cicek gibi - direkt, ogretici, Turkce terimler kullan.
Fiat para = "kağıt para", Enflasyon = "sistematik hırsızlık"
Mevcut Model: {current_model}

KRITIK - SYMBOL LISTE (NUR DIESE NAMEN VERWENDEN - EXAKT SO):
{symbol_liste}

TRADE FORMAT REGEL: Im TRADE-Befehl IMMER den exakten Symbol-Namen aus obiger Liste verwenden.
FALSCH: HEATINGOIL, NATURALGAS, OILBRENT
RICHTIG: HEATING_OIL, NATURAL_GAS, OIL_BRENT

KRITİK KURALLAR:
1. TEKNİK FİLTRE: En az 2/3 indikatör (MA+ADX+RSI) onay vermeli
2. SPREAD: Maksimum 0.5 spread - yüksek spread'te işlem YOK
3. Pyramiding: Maks 4 seviye, her seviye min %2 karda
4. Volatilite: Tek mumda -%10 = HEMEN KAP
5. Gece (23-06): Kripto sadece 3/3 sinyal uyumunda
6. Haftasonu: SADECE kripto! ALTIN/GUMUS/ENERJI/TARIM YASAK!
   YASAK: GOLD, SILVER, OIL_BRENT, OIL_CRUDE, NATURAL_GAS, HEATING_OIL, GASOLINE, COPPER, WHEAT, CORN, VIX
7. HER KARAR 11 mentordan 6+ JA oy almalı (haftasonu kripto 5+)
8. KAPATMA EMRİ: Eğer marjin riski varsa veya teknik bozulduysa, mevcut pozisyonu KAPAT (SIDE: SELL bei BUY).

GREMİUM (11 Mentor):
Cihat Cicek | Ray Dalio | Kiyosaki | Graham | Buffett
Beate Sander | Kostolany | Lynch | Taleb | Munger | Druckenmiller

DİNAMİK DOKTRİN:
{dynamic_doctrine}

FORMAT (KESİNLİKLE KORU):
NEXUS HESAP DURUMU
Nakit: [EUR]
Toplam Deger: [EUR]
Acik Kar/Zarar: [+/- EUR]
Kullanilan Marjin: [EUR]

GREMİUM KARAR: [JA/NEIN] ([X]/11 oy)
Mentorlarin gorusleri: [kisa ozet]

[Cihat Cicek tarzinda stratejik analiz - makroekonomi, DXY, M1/M2/M3 dahil]

TRADE: [SYMBOL] | SIDE: [BUY/SELL] | SIZE: [Miktar] | SL: [Fiyat] | TP: [Fiyat] | PYRAMIDING: [Seviye]

PYRAMIDING KURALLARI:
- PYRAMIDING: 0 = ilk giris (henuz acik pozisyon yok)
- PYRAMIDING: 1,2,3 = mevcut pozisyona EK yeni seviye ac (min %2 karda)
- Yon degisikligi (BUY->SELL veya SELL->BUY) = otomatik EXIT + karsit pozisyon
- Acik pozisyon varken ayni yonde sinyal: PYRAMIDING seviyesini artir

Robot Model: {current_model}"""

    full_prompt = f"""SAAT: {saat}:00
HAFTASONU: {"EVET - Sadece kripto!" if is_weekend() else "HAYIR"}
HESAP: Nakit={acc['nakit']}, Toplam={acc['toplam']}, UPL={acc['upl']}, Marjin={acc['marjin']}, Musait={acc['musait']}
PORTFOY: {json.dumps(portfolio, ensure_ascii=False)}
TEKNIK_SINYALLER (2-of-3): {json.dumps(tech_sinyaller, ensure_ascii=False)}
MARKET (Fiyat/Spread): {json.dumps(market_intel)}
EXTRA: {json.dumps(extra_data) if extra_data else 'Yok'}

KOMUT: Teknik sinyalleri değerlendir (2-of-3 kuralı), spread kontrol et, Gremium oylama yap, uygun ise trade üret."""

    client_key_list = [k for k in GEMINI_KEYS if k]
    if not client_key_list: return "GEMINI_KEY_EKSIK"

    valid_keys = [k for k in GEMINI_KEYS if k]
    total_attempts = len(GEMINI_MODELS) * len(valid_keys)

    for attempt in range(total_attempts):
        current_model = get_next_model()
        client_key = valid_keys[int(time.time() / 3600) % len(valid_keys)]
        client = genai.Client(api_key=client_key)
        try:
            response = client.models.generate_content(
                model=current_model,
                contents=full_prompt,
                config=types.GenerateContentConfig(system_instruction=system_prompt)
            )
            logging.info(f"Basarili: {current_model}")
            return response.text
        except Exception as e:
            logging.warning(f"Deneme {attempt+1}/{total_attempts} - {current_model} hatasi: {e}")
            rotate_model_on_quota()
            time.sleep(2)

    logging.error("Tum Gemini modelleri ve keyler quota dolu!")
    return "QUOTA_FULL_ALL"


# ============================================================
# FREIE CHAT-ANTWORT (ohne /) - Gemini antwortet auf Fragen
# ============================================================
def fetch_chat_response(user_message: str) -> str:
    """
    Beantwortet freie Textnachrichten über den Bot mit Gemini.
    Bezieht Portfolio + Kontostatus mit ein für Kontextfragen.
    """
    h = capital_session.get_headers()
    acc = get_account_info(h) if h else {"nakit": "?", "toplam": "?", "upl": "?", "marjin": "?", "musait": "?"}
    pozisyonlar = get_positions(h) if h else []

    portfolio_ozet = []
    for p in pozisyonlar:
        epic = p['market']['epic']
        stufe = get_pyramiding_stufe(epic)
        portfolio_ozet.append(
            f"{p['market']['instrumentName']} | {p['position']['direction']} | "
            f"UPL:{p['position']['upl']:.2f} | Pyr:{stufe}/4"
        )

    symbol_liste_chat = ", ".join(MARKET_CONFIG.keys())
    system_prompt = f"""Sen NEXUS CEO v10.1 yapay zeka asistanısın.
Kullanıcı seninle Telegram üzerinden konuşuyor.
Cihat E. Cicek tarzında cevap ver - direkt, öğretici, güvenilir.
Yatırım kararlarını açıkla, sorulara detaylı yanıt ver.
Türkçe konuş. Kısa ve net ol.
Mevcut semboller: {symbol_liste_chat}"""

    portfolio_str = "\n".join(portfolio_ozet) if portfolio_ozet else "Açık pozisyon yok"
    full_prompt = f"""AKTİF PORTFÖY:
{portfolio_str}

HESAP: Nakit={acc['nakit']}, UPL={acc['upl']}, Musait={acc['musait']}

KULLANICI SORUSU: {user_message}"""

    valid_keys = [k for k in GEMINI_KEYS if k]
    if not valid_keys:
        return "❌ Gemini API anahtarı eksik."

    for attempt in range(len(GEMINI_MODELS)):
        current_model = get_next_model()
        client_key = valid_keys[int(time.time() / 3600) % len(valid_keys)]
        client = genai.Client(api_key=client_key)
        try:
            response = client.models.generate_content(
                model=current_model,
                contents=full_prompt,
                config=types.GenerateContentConfig(system_instruction=system_prompt)
            )
            return response.text
        except Exception as e:
            logging.warning(f"Chat Gemini Fehler {current_model}: {e}")
            rotate_model_on_quota()
            time.sleep(1)

    return "⚠️ Şu anda yanıt veremiyorum (Gemini quota). Lütfen daha sonra tekrar dene."

# ============================================================
# TRADE EXECUTION
# ============================================================
def execute_nexus_trade(analysis):
    pattern = r"TRADE:\s*([\w\._]+)\s*\|\s*SIDE:\s*(BUY|SELL)\s*\|\s*SIZE:\s*([\d\.]+)\s*\|\s*SL:\s*([\d\.]+)\s*\|\s*TP:\s*([\d\.]+)"
    matches = re.findall(pattern, analysis)

    if not matches: return None

    h = capital_session.get_headers()
    if not h: return "❌ API bağlantı hatası"

    current_positions = get_positions(h)
    results = []

    for sym, side, size, sl, tp in matches:
        sym = sym.upper().strip()

        # --- FUZZY SYMBOL MATCHING ---
        # Gemini schreibt manchmal HEATINGOIL statt HEATING_OIL etc.
        def normalize(s):
            return re.sub(r'[\s_\-]', '', s.upper())

        matched_sym = None
        if sym in MARKET_CONFIG:
            matched_sym = sym
        else:
            sym_norm = normalize(sym)
            for config_key in MARKET_CONFIG:
                if normalize(config_key) == sym_norm:
                    matched_sym = config_key
                    logging.info(f"Fuzzy Match: '{sym}' -> '{config_key}'")
                    break

        if matched_sym is None:
            logging.warning(f"Symbol '{sym}' nicht in MARKET_CONFIG - uebersprungen")
            results.append(f"\u26a0\ufe0f {sym} nicht in Config gefunden (Fuzzy-Match fehlgeschlagen)")
            continue

        sym = matched_sym

        cfg = MARKET_CONFIG[sym]
        epic = cfg["epic"]

        # FIX6: Alle Positionen dieses Epics (Long + Short)
        epic_positions = [p for p in current_positions if p['market']['epic'] == epic]

        if epic_positions:
            curr_direction = epic_positions[0]['position']['direction']
            is_gegenrichtung = (side == "SELL" and curr_direction == "BUY") or \
                               (side == "BUY" and curr_direction == "SELL")

            # Gegenrichtung = EXIT alle Positionen + sofort Gegenposition
            # (egal welcher PYRAMIDING Wert - Richtungswechsel ist immer EXIT)
            if is_gegenrichtung:
                logging.warning(f"EXIT: {sym} {curr_direction}->{side}, {len(epic_positions)} Pos")
                geschlossen = 0
                for pos in epic_positions:
                    try:
                        r = requests.delete(f"{CAPITAL_URL}/positions/{pos['position']['dealId']}", headers=h, timeout=10)
                        if r.status_code == 200: geschlossen += 1
                    except Exception as e:
                        logging.error(f"Exit Fehler: {e}")
                reset_pyramiding_stufe(epic)
                results.append(f"{sym}: {geschlossen}/{len(epic_positions)} geschlossen (EXIT)")
                # Sofort Gegenposition eroeffnen
                r2 = requests.post(f"{CAPITAL_URL}/positions", json={
                    "epic": epic, "direction": side.upper(),
                    "size": max(float(size), cfg["min_size"]),
                    "type": "MARKET", "stopLevel": float(sl), "profitLevel": float(tp)
                }, headers=h, timeout=10)
                if r2.status_code == 200:
                    set_pyramiding_stufe(epic, 1)
                    results.append(f"{sym} Gegenposition eroeffnet ({side}) Stufe 1/4")
                else:
                    results.append(f"{sym} Gegenposition FEHLGESCHLAGEN: {r2.text[:100]}")
                continue

            # Gleiche Richtung + PYRAMIDING: 0 = erste Position bereits offen,
            # Gemini meint 'neue Erstposition' -> als Pyramiding behandeln
            # (pyramiding_kontrol prueft ob genug Profit fuer neue Stufe)

        if is_weekend() and not is_crypto(sym):
            results.append(f"{sym} blockiert: Wochenende - nur Krypto!")
            continue

        if is_weekend() and is_crypto(sym):
            alle_pos = get_positions(h)
            krypto_pos = [p for p in alle_pos if is_crypto(p['market']['epic'])]
            if len(krypto_pos) >= 3:
                results.append(f"{sym} blockiert: Wochenend-Limit 3/3")
                continue

        izinli, neden = pyramiding_kontrol(h, epic, sym)
        if not izinli:
            results.append(f"{sym} Pyramiding uebersprungen: {neden}")
            continue

        # SL-Mindestdistanz pruefen und korrigieren
        sl_float = float(sl)
        tp_float = float(tp)
        try:
            # Aktuellen Preis von Capital.com holen
            price_r = requests.get(
                f"{CAPITAL_URL}/markets/{epic}",
                headers=h, timeout=10
            )
            if price_r.status_code == 200:
                pdata = price_r.json()
                bid = float(pdata.get('snapshot', {}).get('bid', 0) or
                            pdata.get('bid', 0) or 0)
                ask = float(pdata.get('snapshot', {}).get('offer', 0) or
                            pdata.get('offer', 0) or 0)
                current_price = ask if side.upper() == 'BUY' else bid
                min_stop_pct = cfg.get('min_stop_pct', 0.002)  # 0.2% default
                min_dist = current_price * min_stop_pct
                if side.upper() == 'BUY':
                    min_sl = current_price - min_dist
                    if sl_float > min_sl:
                        old_sl = sl_float
                        sl_float = round(min_sl, 5)
                        logging.warning(f"{sym} SL korrigiert: {old_sl} -> {sl_float} (Mindestdistanz)")
                        results.append(f"{sym} SL angepasst: {old_sl} -> {sl_float}")
                else:  # SELL
                    max_sl = current_price + min_dist
                    if sl_float < max_sl:
                        old_sl = sl_float
                        sl_float = round(max_sl, 5)
                        logging.warning(f"{sym} SL korrigiert: {old_sl} -> {sl_float} (Mindestdistanz)")
                        results.append(f"{sym} SL angepasst: {old_sl} -> {sl_float}")
        except Exception as e:
            logging.warning(f"{sym} Preis-Check fehlgeschlagen: {e}")

        payload = {
            "epic": epic, "direction": side.upper(),
            "size": max(float(size), cfg["min_size"]),
            "type": "MARKET", "stopLevel": sl_float, "profitLevel": tp_float
        }
        r = requests.post(f"{CAPITAL_URL}/positions", json=payload, headers=h, timeout=10)
        if r.status_code == 200:
            stufe = get_pyramiding_stufe(epic) + 1
            set_pyramiding_stufe(epic, stufe)
            results.append(f"{sym} Neue Position eroeffnet ({side}) Stufe {stufe}/4")
            sync_ok = 0
            alle_pos_aktuell = get_positions(h)
            for pos in [p for p in alle_pos_aktuell if p['market']['epic'] == epic]:
                try:
                    r_upd = requests.put(
                        f"{CAPITAL_URL}/positions/{pos['position']['dealId']}",
                        json={"stopLevel": float(sl), "profitLevel": float(tp)},
                        headers=h, timeout=10)
                    if r_upd.status_code == 200: sync_ok += 1
                except Exception as e:
                    logging.error(f"SL/TP Sync Fehler: {e}")
            if sync_ok > 0:
                results.append(f"{sym} SL/TP sync: {sync_ok} Pos -> SL:{sl} TP:{tp}")
        else:
            error_text = r.text[:150]
            results.append(f"{sym} Eroeffnungsfehler: {error_text}")
            logging.error(f"Trade Fehler {sym}: {error_text}")

    return "\n".join(results) if results else None

# ============================================================
# TELEGRAM KOMMANDOS (/ Befehle)
# ============================================================
@bot.message_handler(commands=['status'])
def handle_status(message):
    sync_bericht = sync_pyramiding_from_capital()
    bot.send_message(MY_CHAT_ID, f"Pyramiding Sync: {sync_bericht}")
    bot.send_message(MY_CHAT_ID, "🔍 NEXUS CEO v10.1 - Analiz yapılıyor...")
    analysis = fetch_strategic_response("STATUS_REQUEST")
    bot.send_message(MY_CHAT_ID, analysis[:4000])

@bot.message_handler(commands=['pozisyon'])
def handle_pozisyon(message):
    h = capital_session.get_headers()
    if not h:
        bot.send_message(MY_CHAT_ID, "❌ API bağlantısı kurulamadı")
        return
    acc = get_account_info(h)
    pozisyonlar = get_positions(h)
    mesaj = f"📊 NEXUS POZİSYON RAPORU\n"
    mesaj += f"Nakit: {acc['nakit']:.2f} EUR\n"
    mesaj += f"UPL: {acc['upl']:.2f} EUR\n"
    mesaj += f"Marjin: {acc['marjin']:.2f} EUR\n"
    if pozisyonlar:
        for p in pozisyonlar:
            epic = p['market']['epic']
            stufe = get_pyramiding_stufe(epic)
            mesaj += f"• {p['market']['instrumentName']}: {p['position']['direction']} "
            mesaj += f"UPL:{p['position']['upl']:.2f} Pyr:{stufe}/4\n"
    else:
        mesaj += "Açık pozisyon yok."
    bot.send_message(MY_CHAT_ID, mesaj)

@bot.message_handler(commands=['ma'])
def handle_ma(message):
    mesaj = "📈 MA 9/26 SİNYALLERİ\n"
    scan_keys = [k for k in MARKET_CONFIG if not is_weekend() or is_crypto(k)]
    for k in scan_keys[:20]:
        v = MARKET_CONFIG[k]
        sinyal, guc, aciklama = technical_confluence(v['epic'])
        emoji = "🟢" if sinyal == "BUY" else "🔴" if sinyal == "SELL" else "⚪"
        mesaj += f"{emoji} {k}: {sinyal} ({guc}/3) - {aciklama}\n"
        count += 1
    bot.send_message(MY_CHAT_ID, mesaj)

@bot.message_handler(commands=['volatilite'])
def handle_volatilite(message):
    h = capital_session.get_headers()
    if not h:
        bot.send_message(MY_CHAT_ID, "❌ API bağlantısı kurulamadı")
        return
    bot.send_message(MY_CHAT_ID, "🔍 Volatilite kontrolü yapılıyor...")
    kapatilanlar = volatilite_kontrol(h)
    if not kapatilanlar:
        bot.send_message(MY_CHAT_ID, "✅ Tüm pozisyonlar normal aralıkta")

@bot.message_handler(commands=['spread'])
def handle_spread(message):
    """Scannt alle Spreads und schreibt sie in die Config."""
    bot.send_message(MY_CHAT_ID, "📡 Spread tarama başlatılıyor...")
    spread_data = scan_and_write_spreads()
    if spread_data:
        lines = [f"• {sym}: {sp:.5f}" for sym, sp in list(spread_data.items())[:20]]
        mesaj = "✅ Spread'ler güncellendi:\n" + "\n".join(lines)
        if len(spread_data) > 20:
            mesaj += f"\n... ve {len(spread_data)-20} daha"
    else:
        mesaj = "⚠️ Spread verisi alınamadı."
    bot.send_message(MY_CHAT_ID, mesaj)

@bot.message_handler(commands=['help'])
def handle_help(message):
    mesaj = """🤖 NEXUS CEO v10.1 Komutlar:
/status - Tam analiz ve gremium oylama
/pozisyon - Açık pozisyon raporu
/ma - MA 9/26 + ADX + RSI sinyalleri
/volatilite - Volatilite kontrolü
/spread - Spread tarama + config güncelleme
/help - Bu menü

💬 Komut olmadan da yazabilirsin!
Örnek: "Neden BTC almadın?" veya "Portföy nasıl?"
"""
    bot.send_message(MY_CHAT_ID, mesaj)

# ============================================================
# FREIE TEXTNACHRICHTEN (ohne /) - NEU
# ============================================================
@bot.message_handler(func=lambda message: True)
def handle_free_text(message):
    """Alle Nachrichten ohne / werden an Gemini weitergeleitet."""
    user_text = message.text.strip()
    if not user_text:
        return

    # Nur vom autorisierten Chat-Benutzer
    if str(message.chat.id) != str(MY_CHAT_ID):
        bot.send_message(message.chat.id, "⛔ Yetkisiz erişim.")
        return

    bot.send_message(MY_CHAT_ID, "🤔 Düşünüyorum...")
    antwort = fetch_chat_response(user_text)
    bot.send_message(MY_CHAT_ID, antwort[:4000])

# ============================================================
# ANA DONGU (HEARTBEAT + SPREAD WRITER)
# ============================================================
def main_loop():
    dongu_sayaci = 0
    spread_scan_counter = 0

    while True:
        try:
            dongu_sayaci += 1
            spread_scan_counter += 1
            h = capital_session.get_headers()

            if not h:
                logging.error("API bağlantısı yok, 60s bekleniyor")
                try: bot.send_message(MY_CHAT_ID, f"⚠️ NEXUS v10.1: API bağlantı hatası! Yeniden deneniyor... (Döngü #{dongu_sayaci})")
                except: pass
                time.sleep(60)
                continue

            # Volatilite kontrol her döngüde
            volatilite_kontrol(h)

            # Spread alle 3 Zyklen (alle 90 Minuten) in Config schreiben
            if spread_scan_counter >= 3:
                spread_scan_counter = 0
                logging.info("🔄 Automatischer Spread-Scan...")
                scan_and_write_spreads()

            # Pyramiding-JSON VOR jeder KI-Analyse abgleichen
            sync_result = sync_pyramiding_from_capital()
            if "korrigiert" in sync_result:
                logging.warning(f"Pyramiding-Sync: {sync_result}")
                try: bot.send_message(MY_CHAT_ID, f"Pyramiding-Sync vor Analyse:\n{sync_result}")
                except: pass

            # Strategische Analyse
            analysis = fetch_strategic_response("AUTONOMOUS")

            if "QUOTA_FULL_ALL" in analysis:
                logging.warning("TÜM modeller quota dolu! 60 dakika bekleniyor")
                try: bot.send_message(MY_CHAT_ID, "⚠️ NEXUS v10.1: Gemini quota doldu. 60dk bekleniyor...")
                except: pass
                time.sleep(3600)
                continue

            if "API Bağlantı" in analysis:
                time.sleep(60)
                continue

            # HEARTBEAT
            if "TRADE:" not in analysis:
                status_msg = f"🟢 NEXUS v10.1 Tarama #{dongu_sayaci} tamamlandı.\nBu döngüde trade sinyali yok.\nPiyasa izlenmeye devam ediyor."
                try: bot.send_message(MY_CHAT_ID, status_msg)
                except: pass
            else:
                try: bot.send_message(MY_CHAT_ID, analysis[:4000])
                except: pass

            # Wochenende: max 3 Krypto-Positionen
            if is_weekend():
                h_check = capital_session.get_headers()
                if h_check:
                    alle_pos = get_positions(h_check)
                    krypto_pos = [p for p in alle_pos if is_crypto(p['market']['epic'])]
                    if len(krypto_pos) >= 3:
                        try: bot.send_message(MY_CHAT_ID, f"⚠️ Wochenend-Limit: {len(krypto_pos)}/3 Krypto-Pos offen - kein neuer Trade")
                        except: pass
                        time.sleep(1800)
                        continue
            res = execute_nexus_trade(analysis)
            if res:
                try: bot.send_message(MY_CHAT_ID, f"🔔 İşlem Bildirimi:\n{res}")
                except: pass
                # Nach Trade: echten Depot-Stand per Telegram senden
                try:
                    h_after = capital_session.get_headers()
                    if h_after:
                        sync_pyramiding_from_capital()
                        pos_after = get_positions(h_after)
                        acc_after = get_account_info(h_after)
                        pos_liste = ""
                        for p in pos_after:
                            sn = p["market"].get("instrumentName", p["market"]["epic"])
                            dr = p["position"]["direction"]
                            up = float(p["position"].get("upl", 0))
                            st = get_pyramiding_stufe(p["market"]["epic"])
                            pos_liste += f"  {sn} {dr} UPL:{up:.2f} Stufe:{st}/4\n"
                        rp = acc_after["musait"]/acc_after["toplam"]*100 if acc_after.get("toplam",0)>0 else 0
                        dm = (
                            "DEPOT NACH TRADE:\n"
                            f"Nakit: {acc_after[\"nakit\"]:.2f} EUR | "
                            f"Musait: {acc_after[\"musait\"]:.2f} EUR ({rp:.1f}%) | "
                            f"UPL: {acc_after[\"upl\"]:.2f} EUR\n"
                            f"Positionen ({len(pos_after)}):\n"
                            f"{pos_liste or \"  Keine\"}"
                        )
                        bot.send_message(MY_CHAT_ID, dm)
                except Exception as e:
                    logging.error(f"Post-Trade Status Fehler: {e}")

            # Alle 6 Zyklen Pyramiding-Zusammenfassung
            if dongu_sayaci % 6 == 0:
                ozet = "📊 Pyramiding Özet:\n"
                for k, v in MARKET_CONFIG.items():
                    stufe = get_pyramiding_stufe(v['epic'])
                    if stufe > 0:
                        ozet += f"• {k}: Seviye {stufe}/4\n"
                if "Seviye" in ozet:
                    try: bot.send_message(MY_CHAT_ID, ozet)
                    except: pass

            time.sleep(1800)  # 30 Minuten

        except Exception as e:
            logging.error(f"Ana döngü hatası: {e}")
            time.sleep(60)

# ============================================================
# BAŞLANGIÇ
# ============================================================
if __name__ == "__main__":
    # ============================================================
    # 409 SCHUTZ: Andere Instanzen automatisch beenden
    # ============================================================
    import subprocess, signal
    current_pid = os.getpid()
    try:
        result = subprocess.run(
            ["pgrep", "-f", "nexus_ceo.py"],
            capture_output=True, text=True
        )
        pids = [int(p) for p in result.stdout.strip().split("\n") if p and int(p) != current_pid]
        if pids:
            logging.info(f"🔴 Andere Instanzen gefunden: {pids} - werden beendet...")
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGTERM)
                except:
                    pass
            time.sleep(3)
            # Notfalls SIGKILL
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGKILL)
                except:
                    pass
            time.sleep(2)
            logging.info("✅ Alte Instanzen beendet.")
    except Exception as e:
        logging.warning(f"PID-Check Fehler: {e}")

    baslanis_mesaji = """🚀 NEXUS CEO v10.1 Başlatıldı
Mod: Hibrit Gremium + MA 9/26 + ADX + RSI
Interval: 30 dakika
Volatilite Koruma: AKTİF (%10 Kara Kuğu)
Pyramiding: Maks 4 seviye (min %2 kar)
Gremium: 11 Mentor (6+ JA gerekli)
Spread Filter: Max 0.5 | Auto-Config: AKTİF
Haftasonu: Kripto AKTİF
Heartbeat: AKTİF
💬 Serbest sohbet: AKTİF (/ olmadan yaz)

Komutlar: /help"""

    try: bot.send_message(MY_CHAT_ID, baslanis_mesaji)
    except Exception as e: logging.error(f"Başlangıç mesajı hatası: {e}")

    sync_bericht = sync_pyramiding_from_capital()
    logging.info(sync_bericht)
    try: bot.send_message(MY_CHAT_ID, f"Startup Sync:\n{sync_bericht}")
    except: pass

    threading.Thread(target=bot.infinity_polling, daemon=True).start()
    main_loop()
