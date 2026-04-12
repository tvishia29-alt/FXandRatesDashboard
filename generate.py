"""
FX RADAR — Data Generation Pipeline
Fetches global yields, commodities, ETFs, vol data, FX, and news.
Outputs data.json which the HTML dashboard reads.
Run locally: python generate.py
Run via GitHub Actions: automatic on schedule
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
import yfinance as yf

# ─── CONFIG ──────────────────────────────────────────────────────────────────
FRED_KEY = os.environ.get("FRED_API_KEY", "")
FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "")
GOV_KEY = os.environ.get("GOV_API_KEY", "")
OUTPUT_FILE = "data.json"

# ─── YIELD CURVE TICKERS (yfinance) ─────────────────────────────────────────
# These are Yahoo Finance tickers for government bond yields globally
YIELD_TICKERS = {
    "US": {
        "1M": "^IRX",     # 3-month T-bill (closest free proxy)
        "2Y": None,        # Will use FRED
        "5Y": "^FVX",
        "10Y": "^TNX",
        "30Y": "^TYX",
    },
    "UK": {
        "2Y": None,
        "5Y": None,
        "10Y": None,  # We'll scrape or use proxy
    },
    "Germany": {  # Proxy for Eurozone
        "2Y": None,
        "5Y": None,
        "10Y": None,
    },
    "Japan": {
        "2Y": None,
        "10Y": None,
    },
}

# FRED series for full US yield curve
FRED_YIELDS = {
    "DGS1MO": "1M",
    "DGS3MO": "3M",
    "DGS6MO": "6M",
    "DGS1": "1Y",
    "DGS2": "2Y",
    "DGS3": "3Y",
    "DGS5": "5Y",
    "DGS7": "7Y",
    "DGS10": "10Y",
    "DGS20": "20Y",
    "DGS30": "30Y",
}

# FRED series for other countries (where available)
FRED_GLOBAL_YIELDS = {
    "Eurozone": {"IRLTLT01DEM156N": "10Y"},  # Germany 10Y (monthly, delayed)
    "UK": {"IRLTLT01GBM156N": "10Y"},
    "Japan": {"IRLTLT01JPM156N": "10Y"},
    "Canada": {"IRLTLT01CAM156N": "10Y"},
    "Australia": {"IRLTLT01AUM156N": "10Y"},
}

# Bond ETFs as yield proxies (price moves inversely to yields)
BOND_ETFS = {
    "TLT": {"name": "US 20Y+ Treasury", "country": "US", "duration": "long"},
    "IEF": {"name": "US 7-10Y Treasury", "country": "US", "duration": "medium"},
    "SHY": {"name": "US 1-3Y Treasury", "country": "US", "duration": "short"},
    "IGLT.L": {"name": "UK Gilts", "country": "UK", "duration": "mixed"},
    "IBGL.L": {"name": "Eurozone Govt Bonds", "country": "Eurozone", "duration": "mixed"},
    "JGBS": {"name": "Japan Govt Bonds", "country": "Japan", "duration": "mixed"},
    "GOVT": {"name": "US Total Treasury", "country": "US", "duration": "mixed"},
}

# ─── EODHD GOVERNMENT BONDS ─────────────────────────────────────────────────
EODHD_BONDS = {
    "US2Y.GBOND":  {"key": "us2y",  "label": "US 2Y",           "country": "US"},
    "US10Y.GBOND": {"key": "us10y", "label": "US 10Y",          "country": "US"},
    "US30Y.GBOND": {"key": "us30y", "label": "US 30Y",          "country": "US"},
    "UK2Y.GBOND":  {"key": "uk2y",  "label": "UK 2Y",           "country": "UK"},
    "UK10Y.GBOND": {"key": "uk10y", "label": "UK 10Y",          "country": "UK"},
    "UK30Y.GBOND": {"key": "uk30y", "label": "UK 30Y",          "country": "UK"},
    "DE2Y.GBOND":  {"key": "de2y",  "label": "Germany 2Y",      "country": "Eurozone"},
    "DE10Y.GBOND": {"key": "de10y", "label": "Germany 10Y",     "country": "Eurozone"},
    "DE30Y.GBOND": {"key": "de30y", "label": "Germany 30Y",     "country": "Eurozone"},
    "JP2Y.GBOND":  {"key": "jp2y",  "label": "Japan 2Y",        "country": "Japan"},
    "JP10Y.GBOND": {"key": "jp10y", "label": "Japan 10Y",       "country": "Japan"},
    "JP30Y.GBOND": {"key": "jp30y", "label": "Japan 30Y",       "country": "Japan"},
    "IT2Y.GBOND":  {"key": "it2y",  "label": "Italy 2Y",        "country": "Eurozone"},
    "IT10Y.GBOND": {"key": "it10y", "label": "Italy 10Y",       "country": "Eurozone"},
    "FR10Y.GBOND": {"key": "fr10y", "label": "France 10Y",      "country": "Eurozone"},
    "ES10Y.GBOND": {"key": "es10y", "label": "Spain 10Y",       "country": "Eurozone"},
    "CA2Y.GBOND":  {"key": "ca2y",  "label": "Canada 2Y",       "country": "Canada"},
    "CA10Y.GBOND": {"key": "ca10y", "label": "Canada 10Y",      "country": "Canada"},
    "AU2Y.GBOND":  {"key": "au2y",  "label": "Australia 2Y",    "country": "Australia"},
    "AU10Y.GBOND": {"key": "au10y", "label": "Australia 10Y",   "country": "Australia"},
    "NZ2Y.GBOND":  {"key": "nz2y",  "label": "New Zealand 2Y",  "country": "New Zealand"},
    "NZ10Y.GBOND": {"key": "nz10y", "label": "New Zealand 10Y", "country": "New Zealand"},
    "SW10Y.GBOND": {"key": "ch10y", "label": "Switzerland 10Y", "country": "Switzerland"},
    "SE10Y.GBOND": {"key": "se10y", "label": "Sweden 10Y",      "country": "Sweden"},
    "NO10Y.GBOND": {"key": "no10y", "label": "Norway 10Y",      "country": "Norway"},
}

# ─── COMMODITY TICKERS ───────────────────────────────────────────────────────
COMMODITIES = {
    "GC=F": {"key": "gold", "name": "Gold", "unit": "$/oz"},
    "SI=F": {"key": "silver", "name": "Silver", "unit": "$/oz"},
    "CL=F": {"key": "wti", "name": "WTI Crude", "unit": "$/bbl"},
    "BZ=F": {"key": "brent", "name": "Brent Crude", "unit": "$/bbl"},
    "NG=F": {"key": "natgas", "name": "Natural Gas", "unit": "$/MMBtu"},
    "HG=F": {"key": "copper", "name": "Copper", "unit": "$/lb"},
    "ZW=F": {"key": "wheat", "name": "Wheat", "unit": "¢/bu"},
    "ZC=F": {"key": "corn", "name": "Corn", "unit": "¢/bu"},
}

# ─── KEY ETFs ────────────────────────────────────────────────────────────────
KEY_ETFS = {
    "SPY": "S&P 500",
    "QQQ": "Nasdaq 100",
    "IWM": "Russell 2000",
    "EEM": "EM Equities",
    "FXI": "China Large Cap",
    "EWJ": "Japan Equities",
    "EWZ": "Brazil Equities",
    "EWG": "Germany Equities",
    "EWU": "UK Equities",
    "UUP": "US Dollar Index",
    "FXE": "Euro ETF",
    "FXY": "Yen ETF",
    "FXB": "GBP ETF",
    "FXA": "AUD ETF",
    "FXC": "CAD ETF",
    "GLD": "Gold ETF",
    "USO": "Oil ETF",
    "DBA": "Agriculture ETF",
    "XLE": "Energy Sector",
    "XLF": "Financials Sector",
}

# ─── VOLATILITY ──────────────────────────────────────────────────────────────
VOL_TICKERS = {
    "^VIX": "VIX (S&P 500 Vol)",
    "^VXN": "VXN (Nasdaq Vol)",
    "^MOVE": "MOVE (Bond Vol)",
    "VIXY": "VIX Short-Term ETF",
}

# ─── FX PAIRS (yfinance format) ─────────────────────────────────────────────
FX_PAIRS = {
    "EURUSD=X": "EURUSD", "GBPUSD=X": "GBPUSD", "USDJPY=X": "USDJPY",
    "USDCHF=X": "USDCHF", "AUDUSD=X": "AUDUSD", "NZDUSD=X": "NZDUSD",
    "USDCAD=X": "USDCAD", "USDSEK=X": "USDSEK", "USDNOK=X": "USDNOK",
    "USDCNY=X": "USDCNY", "USDMXN=X": "USDMXN", "USDBRL=X": "USDBRL",
    "USDZAR=X": "USDZAR", "USDINR=X": "USDINR", "USDKRW=X": "USDKRW",
    "USDTRY=X": "USDTRY", "USDPLN=X": "USDPLN", "USDHUF=X": "USDHUF",
    "USDCZK=X": "USDCZK", "USDSGD=X": "USDSGD", "USDIDR=X": "USDIDR",
    "USDTHB=X": "USDTHB", "DX-Y.NYB": "DXY",
}


def fetch_eodhd_eod(symbol):
    """Fetch latest EOD data from EODHD for a bond/rate symbol."""
    if not GOV_KEY:
        return None
    try:
        url = f"https://eodhd.com/api/eod/{symbol}"
        params = {"api_token": GOV_KEY, "fmt": "json", "order": "d", "limit": 2}
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or len(data) < 1:
            return None
        latest = data[0]
        prev = data[1] if len(data) >= 2 else None
        value = latest.get("close")
        prev_value = prev.get("close") if prev else None
        change = round(value - prev_value, 4) if value is not None and prev_value is not None else None
        pct_change = (
            round((value - prev_value) / prev_value * 100, 4)
            if value is not None and prev_value is not None and prev_value != 0
            else None
        )
        return {
            "value": value,
            "change": change,
            "pct_change": pct_change,
            "date": latest.get("date"),
        }
    except Exception as e:
        print(f"  EODHD error ({symbol}): {e}")
        return None


def fetch_fred(series_id, limit=5):
    """Fetch latest value from FRED API."""
    if not FRED_KEY:
        print(f"  FRED key missing for {series_id}")
        return None
    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": FRED_KEY,
            "file_type": "json",
            "sort_order": "desc",
            "limit": limit,
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        obs = [o for o in data.get("observations", []) if o["value"] != "."]
        if obs:
            return float(obs[0]["value"])
        print(f"  No valid FRED obs for {series_id}")
    except Exception as e:
        print(f"  FRED error ({series_id}): {e}")
    return None


def fetch_yf_quotes(tickers_dict, period="5d"):
    """Fetch quotes from Yahoo Finance for a dict of ticker: name."""
    results = {}
    ticker_list = list(tickers_dict.keys())

    try:
        # Batch download for efficiency
        data = yf.download(ticker_list, period=period, progress=False, threads=True)

        for ticker in ticker_list:
            name = tickers_dict[ticker]
            try:
                if len(ticker_list) == 1:
                    close_series = data["Close"]
                else:
                    close_series = data["Close"][ticker]

                closes = close_series.dropna()
                if len(closes) >= 1:
                    last = round(float(closes.iloc[-1]), 4)
                    prev = round(float(closes.iloc[-2]), 4) if len(closes) >= 2 else None
                    chg = round((last / prev - 1) * 100, 2) if prev and prev > 0 else None

                    # Calculate 1W, 1M changes if we have enough data
                    results[name] = {
                        "price": last,
                        "prev_close": prev,
                        "change_pct": chg,
                        "timestamp": str(closes.index[-1]),
                    }
            except Exception as e:
                print(f"  yfinance parse error ({ticker}): {e}")

    except Exception as e:
        print(f"  yfinance batch error: {e}")

    return results


def fetch_yf_history(tickers_dict, period="3mo"):
    """Fetch longer history for calculating 1W, 1M, YTD changes."""
    results = {}
    ticker_list = list(tickers_dict.keys())

    try:
        data = yf.download(ticker_list, period=period, progress=False, threads=True)

        for ticker in ticker_list:
            name = tickers_dict[ticker]
            try:
                if len(ticker_list) == 1:
                    closes = data["Close"].dropna()
                else:
                    closes = data["Close"][ticker].dropna()

                if len(closes) < 2:
                    continue

                last = float(closes.iloc[-1])
                d1 = float(closes.iloc[-2]) if len(closes) >= 2 else None
                w1 = float(closes.iloc[-6]) if len(closes) >= 6 else None
                m1 = float(closes.iloc[-22]) if len(closes) >= 22 else None

                results[name] = {
                    "price": round(last, 4),
                    "d1_pct": round((last / d1 - 1) * 100, 2) if d1 else None,
                    "w1_pct": round((last / w1 - 1) * 100, 2) if w1 else None,
                    "m1_pct": round((last / m1 - 1) * 100, 2) if m1 else None,
                }
            except Exception as e:
                print(f"  History parse error ({ticker}): {e}")

    except Exception as e:
        print(f"  History batch error: {e}")

    return results


def fetch_finnhub_news(category="forex"):
    """Fetch news from Finnhub."""
    if not FINNHUB_KEY:
        return []
    try:
        url = f"https://finnhub.io/api/v1/news?category={category}&token={FINNHUB_KEY}"
        r = requests.get(url, timeout=10)
        articles = r.json()
        return [
            {
                "headline": a.get("headline", ""),
                "source": a.get("source", ""),
                "url": a.get("url", ""),
                "datetime": a.get("datetime", 0),
                "category": category,
            }
            for a in articles[:30]
        ]
    except Exception as e:
        print(f"  Finnhub news error: {e}")
        return []


def fetch_finnhub_calendar():
    """Fetch economic calendar from Finnhub."""
    if not FINNHUB_KEY:
        return []
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        future = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        url = f"https://finnhub.io/api/v1/calendar/economic?from={today}&to={future}&token={FINNHUB_KEY}"
        r = requests.get(url, timeout=10)
        data = r.json()
        events = data.get("economicCalendar", [])
        return [
            {
                "date": e.get("time", "")[:10],
                "time": e.get("time", "")[11:16],
                "ctry": e.get("country", ""),
                "ev": e.get("event", ""),
                "prev": e.get("prev") if e.get("prev") is not None else "\u2014",
                "fcast": e.get("estimate") if e.get("estimate") is not None else "\u2014",
                "act": e.get("actual") if e.get("actual") is not None else "\u2014",
                "imp": "red" if e.get("impact") == "high" else "orange",
            }
            for e in events[:40]
        ]
    except Exception as e:
        print(f"  Finnhub calendar error: {e}")
        return []


def fetch_finviz_calendar():
    """Fetch economic calendar from Finviz — data is embedded as JSON in a script tag."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("  bs4 not installed — skipping Finviz calendar")
        return []

    url = "https://finviz.com/calendar.ashx"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "en-GB,en;q=0.9",
    }

    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        print(f"  Finviz status: {r.status_code}, html length: {len(r.text)}")

        soup = BeautifulSoup(r.text, "html.parser")

        # Calendar data is embedded as JSON in a <script> tag
        entries = []
        for script in soup.find_all("script"):
            txt = script.string or ""
            if "entries" in txt and "calendarId" in txt:
                data = json.loads(txt)
                entries = data.get("data", {}).get("entries", [])
                break

        if not entries:
            print("  Finviz: no JSON entries found in page")
            return []

        rows = []
        for e in entries:
            ev_name = e.get("event", "")
            ref = e.get("reference", "")
            dt = e.get("date", "")
            actual = e.get("actual")
            forecast = e.get("forecast")
            previous = e.get("previous")
            importance = e.get("importance", 1)

            date_str = dt[:10] if dt else ""
            time_str = dt[11:16] if len(dt) > 15 else ""

            # Determine beat/miss using isHigherPositive to know direction
            beat = None
            if actual is not None and forecast is not None:
                try:
                    a_val = float(str(actual).replace("%", "").replace(",", ""))
                    f_val = float(str(forecast).replace("%", "").replace(",", ""))
                    higher_good = e.get("isHigherPositive", 1)
                    if higher_good:
                        beat = "beat" if a_val > f_val else ("miss" if a_val < f_val else "inline")
                    else:
                        beat = "beat" if a_val < f_val else ("miss" if a_val > f_val else "inline")
                except (ValueError, TypeError):
                    pass

            imp = "red" if importance >= 3 else ("orange" if importance >= 2 else "orange")

            rows.append({
                "date": date_str,
                "time": time_str,
                "ctry": "🇺🇸",
                "ev": f"{ev_name}" + (f" ({ref})" if ref else ""),
                "prev": str(previous) if previous is not None else "—",
                "fcast": str(forecast) if forecast is not None else "—",
                "act": str(actual) if actual is not None else "—",
                "imp": imp,
                "beat": beat,
                "source": "finviz",
            })

        print(f"  Finviz parsed rows: {len(rows)}")
        return rows[:60]

    except Exception as e:
        print(f"  Finviz calendar error: {e}")
        return []


def fetch_rss_news():
    """Fetch news from free RSS feeds (no API key needed)."""
    import xml.etree.ElementTree as ET

    feeds = [
        ("https://feeds.content.dowjones.io/public/rss/mw_topstories", "MarketWatch"),
        ("https://feeds.bbci.co.uk/news/business/rss.xml", "BBC Business"),
    ]
    articles = []
    for url, source in feeds:
        try:
            r = requests.get(url, timeout=10, headers={"User-Agent": "FXRadar/1.0"})
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:10]:
                title = item.findtext("title", "")
                link = item.findtext("link", "")
                pub = item.findtext("pubDate", "")
                try:
                    pub_ts = int(datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %Z").timestamp())
                except Exception:
                    pub_ts = 0
                if title:
                    articles.append({
                        "headline": title,
                        "source": source,
                        "url": link,
                        "datetime": pub_ts,
                        "category": "general",
                    })
        except Exception as e:
            print(f"  RSS error ({source}): {e}")
    return articles


def fetch_single_ticker_history(ticker, name, period="3mo"):
    """Fallback: fetch a single ticker individually if batch download fails."""
    try:
        data = yf.download(ticker, period=period, progress=False, threads=False)
        closes = data["Close"].dropna()
        if len(closes) < 2:
            return None
        last = float(closes.iloc[-1])
        d1 = float(closes.iloc[-2]) if len(closes) >= 2 else None
        w1 = float(closes.iloc[-6]) if len(closes) >= 6 else None
        m1 = float(closes.iloc[-22]) if len(closes) >= 22 else None
        return {
            "price": round(last, 4),
            "d1_pct": round((last / d1 - 1) * 100, 2) if d1 else None,
            "w1_pct": round((last / w1 - 1) * 100, 2) if w1 else None,
            "m1_pct": round((last / m1 - 1) * 100, 2) if m1 else None,
        }
    except Exception as e:
        print(f"  Single ticker history error ({ticker}): {e}")
        return None


def main():
    print("=" * 60)
    print(f"FX RADAR Data Generation — {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "yields": {},
        "commodities": {},
        "etfs": {},
        "fx": {},
        "volatility": {},
        "bond_etfs": {},
        "news": [],
        "calendar": [],
        "policy_rates": {},
        "macro": {},
        "sentiment": {},
        "scenarios": {},
        "gov_bonds": {},
    }

    # 1. US YIELD CURVE from FRED (most reliable)
    print("\n[1/8] Fetching US yield curve from FRED...")
    us_yields = {}
    for series_id, tenor in FRED_YIELDS.items():
        val = fetch_fred(series_id)
        if val is not None:
            us_yields[tenor] = val
            print(f"  US {tenor}: {val}%")
        time.sleep(0.2)  # Rate limit
    output["yields"]["US"] = {
        "y1m": us_yields.get("1M"),
        "y3m": us_yields.get("3M"),
        "y6m": us_yields.get("6M"),
        "y1": us_yields.get("1Y"),
        "y2": us_yields.get("2Y"),
        "y3": us_yields.get("3Y"),
        "y5": us_yields.get("5Y"),
        "y7": us_yields.get("7Y"),
        "y10": us_yields.get("10Y"),
        "y20": us_yields.get("20Y"),
        "y30": us_yields.get("30Y"),
    }

    # 2. Global yields from FRED (limited but free)
    print("\n[2/8] Fetching global yields from FRED...")
    for country, series_map in FRED_GLOBAL_YIELDS.items():
        country_yields = {}
        for series_id, tenor in series_map.items():
            val = fetch_fred(series_id)
            if val is not None:
                country_yields[tenor] = val
                print(f"  {country} {tenor}: {val}%")
            time.sleep(0.2)
        if country_yields:
            output["yields"][country] = {
                "y10": country_yields.get("10Y"),
                "y2": country_yields.get("2Y"),
                "y5": country_yields.get("5Y"),
            }

    # 3. Yield proxies from Yahoo Finance (US treasuries)
    print("\n[3/8] Fetching yield data from Yahoo Finance...")
    yf_yields = fetch_yf_quotes({
        "^IRX": "US_3M", "^FVX": "US_5Y", "^TNX": "US_10Y", "^TYX": "US_30Y"
    })
    for name, data in yf_yields.items():
        print(f"  {name}: {data['price']}")
    # Fill missing US yields from Yahoo proxies if FRED missing
    us_out = output["yields"].get("US", {})
    if us_out.get("y3m") is None and yf_yields.get("US_3M"):
        us_out["y3m"] = yf_yields["US_3M"]["price"]
    if us_out.get("y5") is None and yf_yields.get("US_5Y"):
        us_out["y5"] = yf_yields["US_5Y"]["price"]
    if us_out.get("y10") is None and yf_yields.get("US_10Y"):
        us_out["y10"] = yf_yields["US_10Y"]["price"]
    if us_out.get("y30") is None and yf_yields.get("US_30Y"):
        us_out["y30"] = yf_yields["US_30Y"]["price"]
    # Fallback for 2Y: try fetching via yfinance ticker
    if us_out.get("y2") is None:
        print("  US 2Y missing from FRED, trying yfinance fallback...")
        try:
            for sym in ["ZT=F"]:  # 2-Year T-Note futures
                t2 = yf.download(sym, period="5d", progress=False)
                if not t2.empty:
                    c2 = t2["Close"].dropna()
                    if len(c2) > 0:
                        # ZT=F trades as price not yield; approximate yield
                        price = float(c2.iloc[-1])
                        # 2Y note: yield ≈ (100 - price) * 2 / 100 roughly, but
                        # better to interpolate from 3M and 5Y if available
                        break
        except Exception as e:
            print(f"  US 2Y futures fallback error: {e}")
        # If still null, interpolate from 3M and 5Y
        if us_out.get("y2") is None:
            y3m = us_out.get("y3m")
            y5 = us_out.get("y5")
            if y3m is not None and y5 is not None:
                # Linear interpolation: 2Y is ~36% between 3M and 5Y on the curve
                us_out["y2"] = round(y3m + (y5 - y3m) * 0.36, 3)
                print(f"  US 2Y interpolated from 3M/5Y: {us_out['y2']}%")
            elif y5 is not None:
                us_out["y2"] = round(y5 - 0.15, 3)  # rough estimate
                print(f"  US 2Y estimated from 5Y: {us_out['y2']}%")
    if us_out.get("y2") is None:
        print("  ⚠ US 2Y still null — carry/spread logic will be limited")
    output["yields"]["US"] = us_out
    output["yields"]["US_yf"] = yf_yields

    # 4. Commodities
    print("\n[4/8] Fetching commodities...")
    commod_data_raw = fetch_yf_history({k: v["name"] for k, v in COMMODITIES.items()})
    commod_data = {}
    print("  Commodity raw keys:", list(commod_data_raw.keys()))
    for ticker, info in COMMODITIES.items():
        name = info["name"]
        key = info["key"]
        row = commod_data_raw.get(name)
        if row:
            row["unit"] = info.get("unit", "")
            commod_data[key] = row
            print(f"  {name}: ${row['price']} ({row.get('d1_pct', '?')}%)")
        else:
            # Fallback: try single ticker download
            single = fetch_single_ticker_history(ticker, name)
            if single:
                single["unit"] = info.get("unit", "")
                commod_data[key] = single
                print(f"  {name} (fallback): ${single['price']} ({single.get('d1_pct', '?')}%)")
    output["commodities"] = commod_data

    # 5. Key ETFs
    print("\n[5/8] Fetching ETFs...")
    etf_data = fetch_yf_history(KEY_ETFS)
    for name, data in etf_data.items():
        print(f"  {name}: ${data['price']} ({data.get('d1_pct', '?')}%)")
    output["etfs"] = etf_data

    # 6. Bond ETFs
    print("\n[6/8] Fetching bond ETFs...")
    bond_data = fetch_yf_history({k: v["name"] for k, v in BOND_ETFS.items()})
    for name, data in bond_data.items():
        info = next((v for k, v in BOND_ETFS.items() if v["name"] == name), {})
        data["country"] = info.get("country", "")
        data["duration"] = info.get("duration", "")
    output["bond_etfs"] = bond_data

    # 7. Volatility
    print("\n[7/8] Fetching volatility data...")
    vol_data = fetch_yf_quotes(VOL_TICKERS)
    for name, data in vol_data.items():
        print(f"  {name}: {data['price']}")
    output["volatility"] = vol_data

    # 8. FX
    print("\n[8/8] Fetching FX rates...")
    fx_data = fetch_yf_history(FX_PAIRS)
    for name, data in fx_data.items():
        print(f"  {name}: {data['price']} ({data.get('d1_pct', '?')}%)")
    output["fx"] = fx_data

    # 9. News (Finnhub + RSS)
    print("\n[9] Fetching news...")
    finnhub_news = fetch_finnhub_news("forex") + fetch_finnhub_news("general")
    rss_news = fetch_rss_news()
    all_news = finnhub_news + rss_news
    # Deduplicate by headline
    seen = set()
    unique_news = []
    for n in all_news:
        key = n["headline"][:50]
        if key not in seen:
            seen.add(key)
            unique_news.append(n)
    output["news"] = unique_news[:40]
    print(f"  {len(unique_news)} unique articles")

    # 10. Economic Calendar (Finnhub + Finviz)
    print("\n[10] Fetching calendar...")
    finnhub_cal = fetch_finnhub_calendar()
    finviz_cal = fetch_finviz_calendar()
    print(f"  Finviz rows: {len(finviz_cal)}")
    print(f"  Finnhub rows: {len(finnhub_cal)}")
    # Merge: prefer Finviz for US events (has beat/miss), keep Finnhub for non-US
    if finviz_cal:
        non_us = [e for e in finnhub_cal if e.get("ctry", "US") != "US"]
        output["calendar"] = finviz_cal + non_us
    elif finnhub_cal:
        output["calendar"] = finnhub_cal
    else:
        print("  ⚠ Both calendar sources empty — inserting debug placeholder")
        output["calendar"] = [{
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": "08:30",
            "ctry": "🇺🇸",
            "ev": "[Calendar sources unavailable]",
            "prev": "—",
            "fcast": "—",
            "act": "—",
            "imp": "orange",
            "beat": None,
        }]
    print(f"  Final calendar rows: {len(output['calendar'])}")

    # 11. Government bonds (EODHD)
    print("\n[11] Fetching government bonds from EODHD...")
    if GOV_KEY:
        for symbol, info in EODHD_BONDS.items():
            result = fetch_eodhd_eod(symbol)
            if result:
                result["label"] = info["label"]
                result["country"] = info["country"]
                output["gov_bonds"][info["key"]] = result
                v = result["value"]
                c = result.get("change")
                chg_str = f" ({c:+.3f})" if c is not None else ""
                print(f"  {info['label']}: {v}%{chg_str}")
            time.sleep(0.3)  # Rate limit
        print(f"  Fetched {len(output['gov_bonds'])} bonds")

        # Backfill yields from EODHD bonds for the curve/spreads views
        bond_to_yield = {
            "us2y": ("US", "y2"), "us10y": ("US", "y10"), "us30y": ("US", "y30"),
            "uk2y": ("UK", "y2"), "uk10y": ("UK", "y10"), "uk30y": ("UK", "y30"),
            "de2y": ("Eurozone", "y2"), "de10y": ("Eurozone", "y10"), "de30y": ("Eurozone", "y30"),
            "jp2y": ("Japan", "y2"), "jp10y": ("Japan", "y10"), "jp30y": ("Japan", "y30"),
            "it2y": ("Italy", "y2"), "it10y": ("Italy", "y10"),
            "fr10y": ("France", "y10"), "es10y": ("Spain", "y10"),
            "ca2y": ("Canada", "y2"), "ca10y": ("Canada", "y10"),
            "au2y": ("Australia", "y2"), "au10y": ("Australia", "y10"),
            "nz2y": ("New Zealand", "y2"), "nz10y": ("New Zealand", "y10"),
            "ch10y": ("Switzerland", "y10"), "se10y": ("Sweden", "y10"), "no10y": ("Norway", "y10"),
        }
        for bond_key, (country, tenor) in bond_to_yield.items():
            bond = output["gov_bonds"].get(bond_key)
            if bond and bond.get("value") is not None:
                if country not in output["yields"]:
                    output["yields"][country] = {}
                # Only fill if not already set from FRED/yfinance
                if output["yields"][country].get(tenor) is None:
                    output["yields"][country][tenor] = bond["value"]
        print("  Backfilled yields from EODHD bonds")
    else:
        print("  EODHD API key not set — skipping government bonds")

    # Write output
    output_path = Path(OUTPUT_FILE)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    size_kb = output_path.stat().st_size / 1024
    print(f"\n{'=' * 60}")
    print(f"✓ Generated {OUTPUT_FILE} ({size_kb:.1f} KB)")
    print(f"  Yields: {len(output['yields'])} countries")
    print(f"  Commodities: {len(output['commodities'])} instruments")
    print(f"  ETFs: {len(output['etfs'])} funds")
    print(f"  FX: {len(output['fx'])} pairs")
    print(f"  News: {len(output['news'])} articles")
    print(f"  Calendar: {len(output['calendar'])} events")
    print(f"  Gov bonds: {len(output['gov_bonds'])} instruments")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
