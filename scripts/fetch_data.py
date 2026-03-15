#!/usr/bin/env python3
"""
fetch_data.py — Market Cockpit data fetcher
Uses yf.Ticker().history() which always returns a flat DataFrame.
Writes data/snapshot.json for the dashboard to load.
"""

import json, math
from datetime import datetime, timezone
import pytz
import yfinance as yf

# ── INSTRUMENTS ──────────────────────────────────────────────────────────────

GROUPS = [
    {
        "id": "futures", "title": "US Index Futures",
        "section": "macro", "col0": "Contract",
        "instruments": [
            {"ticker": "ES=F",  "label": "ES=F · S&P 500 Futures"},
            {"ticker": "NQ=F",  "label": "NQ=F · Nasdaq 100 Futures"},
            {"ticker": "YM=F",  "label": "YM=F · Dow Futures"},
            {"ticker": "RTY=F", "label": "RTY=F · Russell 2000 Futures"},
        ],
    },
    {
        "id": "vix_dollar", "title": "Volatility & Dollar",
        "section": "macro", "col0": "Instrument",
        "instruments": [
            {"ticker": "^VIX",     "label": "VIX · CBOE Volatility Index"},
            {"ticker": "^VVIX",    "label": "VVIX · Vol of Vol Index"},
            {"ticker": "DX-Y.NYB", "label": "DXY · US Dollar Index"},
            {"ticker": "UUP",      "label": "UUP · Invesco Dollar ETF"},
        ],
    },
    {
        "id": "crypto", "title": "Crypto",
        "section": "macro", "col0": "Asset",
        "instruments": [
            {"ticker": "BTC-USD", "label": "BTC-USD · Bitcoin"},
            {"ticker": "ETH-USD", "label": "ETH-USD · Ethereum"},
            {"ticker": "BNB-USD", "label": "BNB-USD · BNB"},
            {"ticker": "SOL-USD", "label": "SOL-USD · Solana"},
            {"ticker": "XRP-USD", "label": "XRP-USD · Ripple"},
        ],
    },
    {
        "id": "metals", "title": "Precious & Base Metals",
        "section": "macro", "col0": "Metal",
        "instruments": [
            {"ticker": "GC=F", "label": "GC=F · Gold Futures"},
            {"ticker": "SI=F", "label": "SI=F · Silver Futures"},
            {"ticker": "HG=F", "label": "HG=F · Copper Futures"},
            {"ticker": "PL=F", "label": "PL=F · Platinum Futures"},
            {"ticker": "PA=F", "label": "PA=F · Palladium Futures"},
        ],
    },
    {
        "id": "energy", "title": "Energy Commodities",
        "section": "macro", "col0": "Commodity",
        "instruments": [
            {"ticker": "CL=F", "label": "CL=F · WTI Crude Oil"},
            {"ticker": "BZ=F", "label": "BZ=F · Brent Crude Oil"},
            {"ticker": "NG=F", "label": "NG=F · Natural Gas"},
            {"ticker": "RB=F", "label": "RB=F · RBOB Gasoline"},
        ],
    },
    {
        "id": "yields", "title": "US Treasury Yields",
        "section": "macro", "col0": "Tenor", "is_yield": True,
        "instruments": [
            {"ticker": "^IRX", "label": "3M · 3-Month T-Bill"},
            {"ticker": "^FVX", "label": "5Y · 5-Year Note"},
            {"ticker": "^TNX", "label": "10Y · 10-Year Note"},
            {"ticker": "^TYX", "label": "30Y · 30-Year Bond"},
        ],
    },
    {
        "id": "global", "title": "Global Market Indices",
        "section": "macro", "col0": "Index",
        "instruments": [
            {"ticker": "^GSPC",     "label": "SPX · S&P 500"},
            {"ticker": "^HSI",      "label": "HSI · Hang Seng"},
            {"ticker": "^N225",     "label": "N225 · Nikkei 225"},
            {"ticker": "^GDAXI",    "label": "DAX · Germany 40"},
            {"ticker": "^FTSE",     "label": "FTSE · UK 100"},
            {"ticker": "^AXJO",     "label": "ASX · Australia 200"},
            {"ticker": "^STOXX50E", "label": "EURO STOXX 50"},
        ],
    },
    {
        "id": "major_etfs", "title": "Major ETF Stats",
        "section": "equities", "col0": "ETF", "has_trend": True,
        "instruments": [
            {"ticker": "SPY", "label": "SPY · SPDR S&P 500"},
            {"ticker": "QQQ", "label": "QQQ · Nasdaq 100"},
            {"ticker": "IWM", "label": "IWM · Russell 2000"},
            {"ticker": "DIA", "label": "DIA · Dow Jones"},
            {"ticker": "VTI", "label": "VTI · Total Market"},
            {"ticker": "EEM", "label": "EEM · Emerging Markets"},
            {"ticker": "GLD", "label": "GLD · Gold ETF"},
            {"ticker": "TLT", "label": "TLT · 20Y Treasury"},
            {"ticker": "HYG", "label": "HYG · High Yield Bond"},
            {"ticker": "LQD", "label": "LQD · IG Corp Bond"},
        ],
    },
    {
        "id": "sp500_sectors", "title": "S&P 500 Sub-Sector — Ranked by 1W",
        "section": "equities", "col0": "Sector",
        "has_trend": True, "has_rank": True, "sort_by": "1w",
        "instruments": [
            {"ticker": "XLK",  "label": "XLK · Technology",       "holdings": "AAPL MSFT NVDA AVGO ORCL"},
            {"ticker": "XLV",  "label": "XLV · Health Care",       "holdings": "UNH JNJ LLY ABBV MRK"},
            {"ticker": "XLP",  "label": "XLP · Consumer Staples",  "holdings": "PG KO COST PM MO"},
            {"ticker": "XLU",  "label": "XLU · Utilities",         "holdings": "NEE DUK SO D SRE"},
            {"ticker": "XLF",  "label": "XLF · Financials",        "holdings": "JPM BAC WFC GS MS"},
            {"ticker": "XLI",  "label": "XLI · Industrials",       "holdings": "RTX HON UPS CAT DE"},
            {"ticker": "XLE",  "label": "XLE · Energy",            "holdings": "XOM CVX EOG COP SLB"},
            {"ticker": "XLB",  "label": "XLB · Materials",         "holdings": "LIN APD SHW FCX NEM"},
            {"ticker": "XLY",  "label": "XLY · Consumer Discret.", "holdings": "AMZN TSLA MCD HD BKNG"},
            {"ticker": "XLRE", "label": "XLRE · Real Estate",      "holdings": "PLD AMT EQIX CCI SPG"},
            {"ticker": "XLC",  "label": "XLC · Comm. Services",    "holdings": "META GOOGL NFLX DIS T"},
        ],
    },
    {
        "id": "thematic", "title": "Top 10 Thematic ETFs — Ranked by 1W",
        "section": "equities", "col0": "Theme / ETF",
        "has_trend": True, "has_rank": True, "has_price": True,
        "sort_by": "1w", "top_n": 10,
        "instruments": [
            {"ticker": "KWEB", "label": "KWEB · China Internet",   "holdings": "BABA PDD JD BIDU"},
            {"ticker": "ARKK", "label": "ARKK · ARK Innovation",   "holdings": "TSLA COIN ROKU CRISPR"},
            {"ticker": "GDX",  "label": "GDX · Gold Miners",       "holdings": "NEM GOLD AEM WPM"},
            {"ticker": "SOXX", "label": "SOXX · Semiconductors",   "holdings": "NVDA AMD QCOM INTC"},
            {"ticker": "IBB",  "label": "IBB · Biotech",           "holdings": "GILD BIIB VRTX REGN"},
            {"ticker": "XBI",  "label": "XBI · S&P Biotech",       "holdings": "SMMT RXRX DNLI"},
            {"ticker": "ICLN", "label": "ICLN · Clean Energy",     "holdings": "ENPH NEE BEP FSLR"},
            {"ticker": "XHB",  "label": "XHB · Homebuilders",      "holdings": "DHI LEN NVR TOL"},
            {"ticker": "LIT",  "label": "LIT · Lithium & Battery", "holdings": "ALB SQM LTHM LAC"},
            {"ticker": "HACK", "label": "HACK · Cybersecurity",    "holdings": "PANW CRWD FTNT ZS"},
            {"ticker": "SKYY", "label": "SKYY · Cloud Computing",  "holdings": "AMZN MSFT GOOGL SNOW"},
            {"ticker": "JETS", "label": "JETS · Airlines",         "holdings": "AAL DAL UAL LUV"},
            {"ticker": "ROBO", "label": "ROBO · Robotics & AI",    "holdings": "ISRG ABB FANUY"},
            {"ticker": "BOTZ", "label": "BOTZ · Global Robotics",  "holdings": "NVDA ABB FANUY ISRG"},
        ],
    },
    {
        "id": "country_etfs", "title": "Country ETFs — Top 10 by 1W",
        "section": "equities", "col0": "Country / ETF",
        "has_trend": True, "has_rank": True, "has_price": True,
        "sort_by": "1w", "top_n": 10,
        "instruments": [
            {"ticker": "MCHI", "label": "MCHI · China",      "holdings": "TENCENT BABA MEITUAN"},
            {"ticker": "EWG",  "label": "EWG · Germany",     "holdings": "SAP SIE DTE"},
            {"ticker": "EWJ",  "label": "EWJ · Japan",       "holdings": "SONY TM 7203"},
            {"ticker": "EWA",  "label": "EWA · Australia",   "holdings": "BHP RIO CSL"},
            {"ticker": "EWC",  "label": "EWC · Canada",      "holdings": "RY TD ENB"},
            {"ticker": "EWY",  "label": "EWY · South Korea", "holdings": "SAMSUNG SK HYUNDAI"},
            {"ticker": "EWT",  "label": "EWT · Taiwan",      "holdings": "TSMC MEDI UMC"},
            {"ticker": "EWZ",  "label": "EWZ · Brazil",      "holdings": "VALE PETR ITUB"},
            {"ticker": "EWU",  "label": "EWU · UK",          "holdings": "SHEL AZN HSBC"},
            {"ticker": "EWI",  "label": "EWI · Italy",       "holdings": "ENI ISP UCG"},
            {"ticker": "EWL",  "label": "EWL · Switzerland", "holdings": "NESN ROG NOVN"},
            {"ticker": "EWS",  "label": "EWS · Singapore",   "holdings": "DBS OCBC UOB"},
            {"ticker": "EWH",  "label": "EWH · Hong Kong",   "holdings": "AIA HSBC MTR"},
            {"ticker": "INDA", "label": "INDA · India",      "holdings": "RELIANCE INFY TCS"},
        ],
    },
]

# ── HELPERS ──────────────────────────────────────────────────────────────────

def pct_str(v, d=2):
    if v is None: return None
    return f"{'+' if v>=0 else ''}{v:.{d}f}%"

def price_str(v, is_yield=False):
    if v is None: return None
    if is_yield: return f"{v:.3f}%"
    if v >= 1000: return f"{v:,.2f}"
    return f"{v:.4g}"

def trend_arrow(w):
    if w is None: return "→"
    return "↗" if w > 1 else ("↘" if w < -1 else "→")

def bars_array(closes):
    if not closes or len(closes) < 2: return [0,0,0,0,0]
    r = closes[-6:]
    b = [1 if r[i]>r[i-1] else (-1 if r[i]<r[i-1] else 0) for i in range(1,len(r))]
    while len(b) < 5: b.insert(0,0)
    return b[-5:]

# ── CORE FETCH ───────────────────────────────────────────────────────────────

def fetch_ticker(tk, is_yield=False):
    """
    yf.Ticker().history() always returns a flat DataFrame — no MultiIndex ever.
    This is the most reliable yfinance method.
    """
    try:
        hist = yf.Ticker(tk).history(period="1y", interval="1d")
        if hist is None or hist.empty:
            raise ValueError("empty")

        # Always flat columns with .history()
        closes = [float(v) for v in hist["Close"].dropna().tolist()]
        idx    = hist["Close"].dropna().index.tolist()

        if len(closes) < 6:
            raise ValueError("insufficient data")

        price = closes[-1]
        prev  = closes[-2]

        chg_1d = (price - prev) / prev * 100
        chg_1w = (price - closes[-6]) / closes[-6] * 100

        # YTD
        year = datetime.now().year
        ytd  = [(d,v) for d,v in zip(idx,closes) if d.year == year]
        chg_ytd = (price - ytd[0][1]) / ytd[0][1] * 100 if ytd else None

        # 52W high
        hi52      = max(closes)
        chg_52w   = (price - hi52) / hi52 * 100

        # Yield 1D in bps
        if is_yield:
            bps = (chg_1d / 100) * (price / 100) * 10000
            chg_1d_str = f"{'+' if bps>=0 else ''}{bps:.1f}bps"
        else:
            chg_1d_str = pct_str(chg_1d)

        return {
            "ok": True,
            "price":     price_str(price, is_yield),
            "price_raw": price,
            "chg_1d":    chg_1d_str,
            "chg_1d_raw": chg_1d,
            "chg_1w":    pct_str(chg_1w),
            "chg_1w_raw": chg_1w,
            "chg_52w_hi":  pct_str(chg_52w),
            "chg_52w_hi_raw": chg_52w,
            "chg_ytd":   pct_str(chg_ytd),
            "chg_ytd_raw": chg_ytd,
            "bars":      bars_array(closes),
            "trend":     trend_arrow(chg_1w),
        }
    except Exception as e:
        print(f"    FAIL {tk}: {e}")
        return {"ok": False}

def blank_row(tk, label, holdings):
    return {
        "ticker": tk, "label": label, "holdings": holdings,
        "price": None, "price_raw": None,
        "chg_1d": None, "chg_1d_raw": None,
        "chg_1w": None, "chg_1w_raw": None,
        "chg_52w_hi": None, "chg_52w_hi_raw": None,
        "chg_ytd": None, "chg_ytd_raw": None,
        "bars": [0,0,0,0,0], "trend": "→",
    }

def fetch_group(group):
    rows = []
    for instr in group["instruments"]:
        tk, label, holdings = instr["ticker"], instr["label"], instr.get("holdings","")
        r = fetch_ticker(tk, is_yield=group.get("is_yield", False))
        if r["ok"]:
            row = {"ticker": tk, "label": label, "holdings": holdings}
            row.update({k:v for k,v in r.items() if k != "ok"})
            print(f"    OK  {tk:12s} price={r['price']:>12}  1D={str(r['chg_1d']):>10}  1W={str(r['chg_1w']):>10}  YTD={str(r['chg_ytd'])}")
        else:
            row = blank_row(tk, label, holdings)
        rows.append(row)
    return rows

def fetch_breadth():
    print("  [breadth]")
    try:
        vix = float(yf.Ticker("^VIX").history(period="5d")["Close"].dropna().iloc[-1])
    except: vix = None

    try:
        sp  = [float(v) for v in yf.Ticker("^GSPC").history(period="1y")["Close"].dropna().tolist()]
        sp_price, sp_prev = sp[-1], sp[-2]
        sp_hi, sp_lo = max(sp), min(sp)
    except: sp_price = sp_prev = sp_hi = sp_lo = None

    up = 0
    for stk in ["XLK","XLV","XLP","XLU","XLF","XLI","XLE","XLB","XLY","XLRE","XLC"]:
        try:
            c = [float(v) for v in yf.Ticker(stk).history(period="5d")["Close"].dropna().tolist()]
            if len(c)>=2 and c[-1]>c[-2]: up+=1
        except: pass

    total = 11
    rng_pct = 50
    if sp_hi and sp_lo and sp_price:
        r = sp_hi - sp_lo
        if r > 0: rng_pct = round((sp_price - sp_lo) / r * 100)

    fg = 50
    if vix:
        if vix<12: fg=80
        elif vix<16: fg=65
        elif vix<20: fg=50
        elif vix<25: fg=35
        elif vix<30: fg=20
        else: fg=10

    return [
        {"label":"Advancing Sectors",      "value":f"{up}/{total}",                         "dir":"up" if up>=6 else "dn", "sub":"S&P 500 GICS sectors", "pct":round(up/total*100)},
        {"label":"VIX Volatility",         "value":f"{vix:.2f}" if vix else "—",            "dir":"dn" if (vix or 20)>20 else "up", "sub":"Elevated above 20", "pct":min(int(vix or 20),100)},
        {"label":"Fear & Greed Index",     "value":str(fg),                                  "dir":"up" if fg>50 else "dn", "sub":"0=Fear  100=Greed", "pct":fg},
        {"label":"S&P 52W Range Pos.",     "value":f"{rng_pct}%",                            "dir":"up" if rng_pct>50 else "dn", "sub":"Position in 52W range", "pct":rng_pct},
        {"label":"S&P 500",                "value":f"{sp_price:,.2f}" if sp_price else "—",  "dir":"up" if (sp_price and sp_prev and sp_price>sp_prev) else "dn", "sub":"Last close", "pct":50},
        {"label":"S&P 52W High",           "value":f"{sp_hi:,.2f}" if sp_hi else "—",        "dir":"up", "sub":"52-week high", "pct":90},
        {"label":"S&P 52W Low",            "value":f"{sp_lo:,.2f}" if sp_lo else "—",        "dir":"dn", "sub":"52-week low",  "pct":10},
        {"label":"Declining Sectors",      "value":f"{total-up}/{total}",                    "dir":"dn" if up<6 else "up", "sub":"S&P 500 GICS sectors", "pct":round((total-up)/total*100)},
        {"label":"Volatility Regime",      "value":"HIGH" if (vix or 0)>25 else ("MED" if (vix or 0)>18 else "LOW"), "dir":"dn" if (vix or 0)>20 else "up", "sub":f"VIX: {vix:.1f}" if vix else "—", "pct":min(int((vix or 20)*2),100)},
        {"label":"Above 50MA (proxy)",     "value":f"{min(80,max(20,rng_pct+5))}%",          "dir":"up" if rng_pct>45 else "dn", "sub":"S&P sector proxy", "pct":min(80,max(20,rng_pct+5))},
    ]

# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    sgt = pytz.timezone("Asia/Singapore")
    ts  = datetime.now(sgt).strftime("%Y-%m-%d %H:%M SGT")
    print(f"\n{'='*60}\n  Market Cockpit -- Data Fetch\n  {ts}\n{'='*60}\n")

    snap = {"updated": ts, "updated_iso": datetime.now(timezone.utc).isoformat(), "groups": [], "breadth": []}

    for g in GROUPS:
        print(f"\n[{g['id']}] {g['title']}")
        rows = fetch_group(g)
        if g.get("sort_by") == "1w":
            rows.sort(key=lambda r: r.get("chg_1w_raw") or -999, reverse=True)
        if g.get("top_n"):
            rows = rows[:g["top_n"]]
        snap["groups"].append({
            "id": g["id"], "title": g["title"], "section": g["section"],
            "col0": g["col0"], "has_trend": g.get("has_trend",False),
            "has_rank": g.get("has_rank",False), "has_price": g.get("has_price",False),
            "is_yield": g.get("is_yield",False), "rows": rows,
        })

    snap["breadth"] = fetch_breadth()

    with open("data/snapshot.json","w") as f:
        json.dump(snap, f, indent=2)

    total = sum(len(g["rows"]) for g in snap["groups"])
    print(f"\nDone -- {ts}  |  {len(snap['groups'])} groups  |  {total} instruments\n")

if __name__ == "__main__":
    main()
