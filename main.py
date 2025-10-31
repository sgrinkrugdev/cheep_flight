#!/usr/bin/env python3
import os, sys, csv, math, json, requests, traceback
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional
import yaml

AMAD_AUTH_URL = "https://test.api.amadeus.com/v1/security/oauth2/token"
AMAD_SEARCH_URL = "https://test.api.amadeus.com/v2/shopping/flight-offers"

def fmt_dt(dt_iso: str) -> str:
    """Format ISO string like 2025-01-01T12:01:00 to 'Jan 01, 2025 12:01 PM'."""
    try:
        dt_iso = dt_iso.replace("Z", "+00:00")
        return datetime.fromisoformat(dt_iso).strftime("%b %d, %Y %I:%M %p")
    except Exception:
        return dt_iso

def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def daterange(start: date, end: date):
    days = int((end - start).days)
    for i in range(days + 1):
        yield start + timedelta(days=i)

def amadeus_get_token(client_id: str, client_secret: str) -> str:
    resp = requests.post(
        AMAD_AUTH_URL,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
        timeout=20
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def iso(d: date) -> str:
    return d.isoformat()

def safe_get(dct: dict, path: List[str], default=None):
    cur = dct
    try:
        for p in path:
            cur = cur[p]
        return cur
    except Exception:
        return default

def pick_cheapest_offer(offers: List[dict]) -> Optional[dict]:
    best = None
    best_total = math.inf
    for o in offers:
        total = float(safe_get(o, ["price", "grandTotal"], safe_get(o, ["price","total"], "inf")))
        if total < best_total:
            best_total = total
            best = o
    return best

def summarize_offer(offer: dict, carriers: Dict[str, str]) -> Dict[str, Any]:
    price = float(safe_get(offer, ["price", "grandTotal"], safe_get(offer, ["price","total"], "nan")))
    currency = safe_get(offer, ["price", "currency"], "USD")
    validating = safe_get(offer, ["validatingAirlineCodes", 0], "N/A")

    itin0 = safe_get(offer, ["itineraries", 0], {})
    itin1 = safe_get(offer, ["itineraries", 1], {})

    def map_segments(itin):
        segs_out = []
        for seg in safe_get(itin, ["segments"], []) or []:
            cc = str(seg.get("carrierCode", "") or "")
            no = str(seg.get("number", "") or "")
            dep = seg.get("departure", {}) or {}
            arr = seg.get("arrival", {}) or {}
            segs_out.append({
                "carrier_code": cc,
                "carrier_name": carriers.get(cc, cc),
                "flight_number": f"{cc}{no}" if (cc or no) else "",
                "dep_airport": dep.get("iataCode", ""),
                "arr_airport": arr.get("iataCode", ""),
                "dep_at": dep.get("at", ""),
                "arr_at": arr.get("at", ""),
            })
        return segs_out

    outbound_segments = map_segments(itin0)
    return_segments  = map_segments(itin1)

    # First-segment timestamps for quick summary fields (kept for subject/overview)
    out0 = outbound_segments[0] if outbound_segments else {}
    ret0 = return_segments[0]  if return_segments  else {}

    return {
        "price": price,
        "currency": currency,
        "airline": validating,  # validating airline code
        "outbound_segments": outbound_segments,
        "return_segments": return_segments,
        "out_depart": out0.get("dep_at", ""),
        "ret_depart": ret0.get("dep_at", ""),
        "stops_out": max(0, len(outbound_segments) - 1),
        "stops_ret": max(0, len(return_segments) - 1),
    }


def search_cheapest_for_window(token: str, origin: str, dest: str, depart: date, duration: int, 
                               adults: int, cabin: Optional[str], currency: str, max_stops: Optional[int]) -> Optional[Dict[str,Any]]:
    from time import sleep

    return_date = depart + timedelta(days=duration)
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": dest,
        "departureDate": iso(depart),
        "returnDate": iso(return_date),
        "adults": str(adults),
        "currencyCode": currency,
        "max": "20"  # keep; 'sort' is NOT supported on some test endpoints
    }
    if cabin:
        params["travelClass"] = cabin  # ECONOMY, PREMIUM_ECONOMY, BUSINESS, FIRST

    headers = {"Authorization": f"Bearer {token}"}

    # simple retry (once) for 429
    for attempt in (1, 2):
        try:
            resp = requests.get(AMAD_SEARCH_URL, headers=headers, params=params, timeout=30)
        except Exception as e:
            print(f"[debug] {origin}->{dest} {depart} dur={duration}: request failed: {e}")
            return None

        if resp.status_code == 429:
            print(f"[debug] {origin}->{dest} {depart} dur={duration}: 429 rate limited, retrying…")
            sleep(1.5)
            continue

        if resp.status_code >= 400:
            # show first part of body for diagnosis
            body = resp.text[:400].replace("\n", " ")
            print(f"[debug] {origin}->{dest} {depart} dur={duration}: HTTP {resp.status_code} {body}")
            return None

        data = resp.json()
        carriers = data.get("dictionaries", {}).get("carriers", {})
        offers = data.get("data", [])
        pre = len(offers)
        if not offers:
            print(f"[debug] {origin}->{dest} {depart} dur={duration}: offers=0")
            return None

        # Filter by stops if requested
        if max_stops is not None:
            filtered = []
            for o in offers:
                it0 = safe_get(o, ["itineraries", 0, "segments"], [])
                it1 = safe_get(o, ["itineraries", 1, "segments"], [])
                stops0 = max(0, len(it0) - 1)
                stops1 = max(0, len(it1) - 1)
                if stops0 <= max_stops and stops1 <= max_stops:
                    filtered.append(o)
            offers = filtered or offers

        post = len(offers)
        cheapest = pick_cheapest_offer(offers)
        if not cheapest:
            print(f"[debug] {origin}->{dest} {depart} dur={duration}: offers_pre={pre} offers_post={post} cheapest=n/a")
            return None

        summary = summarize_offer(cheapest, carriers)

        print(f"[debug] {origin}->{dest} {depart} dur={duration}: offers_pre={pre} offers_post={post} cheapest={summary['price']} {summary['currency']}")

        summary.update({
            "origin": origin,
            "destination": dest,
            "depart_date": iso(depart),
            "return_date": iso(return_date),
        })
        return summary

    # fell through retries
    print(f"[debug] {origin}->{dest} {depart} dur={duration}: 429 persisted, skipping")
    return None



def run_search(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    ci = os.getenv("AMADEUS_API_KEY") or cfg["amadeus"]["api_key"]
    cs = os.getenv("AMADEUS_API_SECRET") or cfg["amadeus"]["api_secret"]
    token = amadeus_get_token(ci, cs)

    currency = cfg.get("currency", "USD")
    adults = int(cfg.get("adults", 1))
    cabin = cfg.get("cabin")
    max_stops = cfg.get("max_stops", None)
    max_stops = int(max_stops) if max_stops is not None else None

    results = []
    for route in cfg["routes"]:
        origin = route["origin"]
        dest = route["destination"]

        # Coerce YAML-loaded values (which may already be date objects) to strings
        start_raw = route["start_date"]
        end_raw = route["end_date"]
        start = date.fromisoformat(str(start_raw))
        end = date.fromisoformat(str(end_raw))
        durations = route.get("durations", [10])
        for dur in durations:
            dur = int(dur)
            # end - dur ensures the return date is within the window
            for d0 in daterange(start, end - timedelta(days=dur)):
                try:
                    found = search_cheapest_for_window(
                        token, origin, dest, d0, dur, adults, cabin, currency, max_stops
                    )
                except Exception:
                    traceback.print_exc()
                    found = None
                if found:
                    found["duration_days"] = dur
                    found["route_name"] = route.get("name", f"{origin}-{dest}")
                    results.append(found)

    # keep only per-route/duration minimum
    best = {}
    for r in results:
        key = (r["route_name"], r["duration_days"])
        if key not in best or r["price"] < best[key]["price"]:
            best[key] = r
    return list(best.values())

def ensure_log(path: str):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("run_ts,route_name,origin,destination,depart_date,return_date,duration_days,airline,price,currency,stops_out,stops_ret,out_depart,ret_depart\n")

def append_log(path: str, rows: List[Dict[str, Any]]):
    ensure_log(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        for r in rows:
            f.write(",".join([
                datetime.utcnow().isoformat(),
                r["route_name"], r["origin"], r["destination"],
                r["depart_date"], r["return_date"],
                str(r["duration_days"]), r["airline"], str(r["price"]), r["currency"],
                str(r["stops_out"]), str(r["stops_ret"]), r["out_depart"], r["ret_depart"]
            ]) + "\n")

def send_email_sendgrid(subject: str, html: str, to_email: str, from_email: str):
    key = os.getenv("SENDGRID_API_KEY")
    if not key:
        raise RuntimeError("SENDGRID_API_KEY is not set")
    import requests
    r = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": from_email},
            "subject": subject,
            "content": [{"type": "text/html", "value": html}],
        },
        timeout=20
    )
    if r.status_code >= 300:
        raise RuntimeError(f"SendGrid error: {r.status_code} {r.text}")

def build_daily_digest(best, cfg):
    """HTML email body with per-segment details (airline name, flight number, dep/arr times)."""
    def fmt_dt(dt_iso: str) -> str:
        # 2025-01-01T12:01:00[Z] -> 'Jan 01, 2025 12:01 PM'
        try:
            s = (dt_iso or "").replace("Z", "+00:00")
            return datetime.fromisoformat(s).strftime("%b %d, %Y %I:%M %p")
        except Exception:
            return dt_iso or ""

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"<h2>Daily Flight Watcher — {now}</h2>"]

    if not best:
        lines.append("<p>No offers found today.</p>")
        return "\n".join(lines)

    lines.append("<ul>")
    # Sort by route then duration for a tidy email
    for r in sorted(best, key=lambda x: (x.get('route_name',''), x.get('duration_days', 0))):
        price = f"{r.get('price','')} {r.get('currency','')}".strip()
        route = r.get('route_name', '')
        dur   = r.get('duration_days', '')
        dep   = r.get('depart_date', '')
        ret   = r.get('return_date', '')
        stops_out = r.get('stops_out', 0)
        stops_ret = r.get('stops_ret', 0)

        lines.append(
            f"<li><b>{route}</b> — {dur} days — "
            f"<b>{price}</b> "
            f"(stops out/ret: {stops_out}/{stops_ret})<br>"
            f"Window: depart <b>{dep}</b>, return <b>{ret}</b><br>"
        )

        # Outbound segments
        outs = r.get("outbound_segments", [])
        if outs:
            lines.append("<div><u>Outbound</u><ul>")
            for seg in outs:
                flight = seg.get("flight_number", "")
                airline = seg.get("carrier_name", seg.get("carrier_code",""))
                dep_air = seg.get("dep_airport","")
                arr_air = seg.get("arr_airport","")
                dep_at  = fmt_dt(seg.get("dep_at",""))
                arr_at  = fmt_dt(seg.get("arr_at",""))
                lines.append(
                    "<li>"
                    f"Flight <b>{flight}</b> {airline} — "
                    f"Departs <b>{dep_air}</b> {dep_at} — "
                    f"Arrives <b>{arr_air}</b> {arr_at}"
                    "</li>"
                )
            lines.append("</ul></div>")

        # Return segments
        rets = r.get("return_segments", [])
        if rets:
            lines.append("<div><u>Return</u><ul>")
            for seg in rets:
                flight = seg.get("flight_number", "")
                airline = seg.get("carrier_name", seg.get("carrier_code",""))
                dep_air = seg.get("dep_airport","")
                arr_air = seg.get("arr_airport","")
                dep_at  = fmt_dt(seg.get("dep_at",""))
                arr_at  = fmt_dt(seg.get("arr_at",""))
                lines.append(
                    "<li>"
                    f"Flight <b>{flight}</b> {airline} — "
                    f"Departs <b>{dep_air}</b> {dep_at} — "
                    f"Arrives <b>{arr_air}</b> {arr_at}"
                    "</li>"
                )
            lines.append("</ul></div>")

        lines.append("</li>")  # end route item

    lines.append("</ul>")
    return "\n".join(lines)


def main():
    cfg_path = os.getenv("CONFIG_PATH", "config.yaml")
    cfg = load_config(cfg_path)
    # Normalize dates to strings so downstream parsing is stable (PyYAML may load them as date objects)
    for r in cfg.get("routes", []):
        for k in ("start_date", "end_date"):
            if k in r and not isinstance(r[k], str):
                r[k] = str(r[k])

    best = run_search(cfg)
    append_log(cfg.get("log_csv", "cheapest_log.csv"), best)
    subj = "Flight Watcher: Daily cheapest picks"
    html = build_daily_digest(best, cfg)
    to_email = os.getenv("TO_EMAIL") or cfg["email"]["to"]
    from_email = os.getenv("FROM_EMAIL") or cfg["email"]["from"]
    send_email_sendgrid(subj, html, to_email, from_email)
    print("Email sent. Entries:", len(best))

if __name__ == "__main__":
    main()
