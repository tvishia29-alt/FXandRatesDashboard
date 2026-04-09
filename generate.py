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


def fetch_fred(series_id, limit=5):
    """Fetch latest value from FRED API."""
    if not FRED_KEY:
        return None
    try:
        url = f"https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": FRED_KEY,
            "file_type": "json",
            "sort_order": "desc",
            "limit": limit,
        }
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        obs = [o for o in data.get("observations", []) if o["value"] != "."]
        if obs:
            return float(obs[0]["value"])
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
    """Scrape economic calendar from Finviz for richer data with beat/miss info."""
    try:
        from html.parser import HTMLParser

        url = "https://finviz.com/calendar.ashx"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        r = requests.get(url, timeout=15, headers=headers)
        if r.status_code != 200:
            print(f"  Finviz calendar HTTP {r.status_code}")
            return []

        class CalParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.in_table = False
                self.in_row = False
                self.in_cell = False
                self.cells = []
                self.current_cell = ""
                self.rows = []
                self.table_depth = 0
                self.target_table = False

            def handle_starttag(self, tag, attrs):
                attrs_d = dict(attrs)
                if tag == "table" and attrs_d.get("class", "") == "calendar_table":
                    self.target_table = True
                    self.table_depth = 0
                if self.target_table and tag == "table":
                    self.table_depth += 1
                if self.target_table and tag == "tr":
                    self.in_row = True
                    self.cells = []
                if self.target_table and self.in_row and tag == "td":
                    self.in_cell = True
                    self.current_cell = ""

            def handle_endtag(self, tag):
                if self.target_table and tag == "td" and self.in_cell:
                    self.in_cell = False
                    self.cells.append(self.current_cell.strip())
                if self.target_table and tag == "tr" and self.in_row:
                    self.in_row = False
                    if len(self.cells) >= 6:
                        self.rows.append(self.cells[:])
                if self.target_table and tag == "table":
                    self.table_depth -= 1
                    if self.table_depth <= 0:
                        self.target_table = False

            def handle_data(self, data):
                if self.in_cell:
                    self.current_cell += data

        parser = CalParser()
        parser.feed(r.text)

        events = []
        current_date = ""
        today = datetime.now().strftime("%Y-%m-%d")
        for row in parser.rows:
            # Finviz columns: Date, Time, Release, For, Actual, Expected, Prior
            date_str = row[0].strip() if row[0].strip() else current_date
            if date_str:
                current_date = date_str
            time_str = row[1].strip() if len(row) > 1 else ""
            release = row[2].strip() if len(row) > 2 else ""
            period = row[3].strip() if len(row) > 3 else ""
            actual = row[4].strip() if len(row) > 4 else ""
            expected = row[5].strip() if len(row) > 5 else ""
            prior = row[6].strip() if len(row) > 6 else ""

            if not release:
                continue

            # Determine beat/miss
            beat = None
            if actual and expected and actual != "" and expected != "":
                try:
                    a_val = float(actual.replace("%", "").replace(",", ""))
                    e_val = float(expected.replace("%", "").replace(",", ""))
                    if a_val > e_val:
                        beat = "beat"
                    elif a_val < e_val:
                        beat = "miss"
                    else:
                        beat = "inline"
                except ValueError:
                    pass

            events.append({
                "date": current_date,
                "time": time_str,
                "ctry": "US",
                "ev": f"{release}" + (f" ({period})" if period else ""),
                "prev": prior if prior else "\u2014",
                "fcast": expected if expected else "\u2014",
                "act": actual if actual else "\u2014",
                "imp": "red",
                "beat": beat,
                "source": "finviz",
            })

        print(f"  Finviz: {len(events)} calendar events scraped")
        return events
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
    # Merge: prefer Finviz for US events (has beat/miss), keep Finnhub for non-US
    if finviz_cal:
        # Use Finviz as primary, add non-US Finnhub events
        non_us = [e for e in finnhub_cal if e.get("ctry", "US") != "US"]
        output["calendar"] = finviz_cal + non_us
    else:
        output["calendar"] = finnhub_cal
    print(f"  {len(output['calendar'])} total events")

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
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
