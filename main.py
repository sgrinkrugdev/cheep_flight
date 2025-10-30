#!/usr/bin/env python3
import os, sys, csv, math, json, requests, traceback
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional
import yaml

AMAD_AUTH_URL = "https://test.api.amadeus.com/v1/security/oauth2/token"
AMAD_SEARCH_URL = "https://test.api.amadeus.com/v2/shopping/flight-offers"

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

def summarize_offer(offer: dict) -> Dict[str, Any]:
    price = float(safe_get(offer, ["price", "grandTotal"], safe_get(offer, ["price","total"], "nan")))
    currency = safe_get(offer, ["price", "currency"], "USD")
    validating = safe_get(offer, ["validatingAirlineCodes", 0], "N/A")
    itin0 = safe_get(offer, ["itineraries", 0], {})
    itin1 = safe_get(offer, ["itineraries", 1], {})
    dep0 = safe_get(itin0, ["segments", 0, "departure", "at"], "")
    dep1 = safe_get(itin1, ["segments", 0, "departure", "at"], "")
    return {
        "price": price,
        "currency": currency,
        "airline": validating,
        "out_depart": dep0,
        "ret_depart": dep1,
        "stops_out": max(0, len(safe_get(itin0, ["segments"], [])) - 1),
        "stops_ret": max(0, len(safe_get(itin1, ["segments"], [])) - 1),
    }

def search_cheapest_for_window(token: str, origin: str, dest: str, depart: date, duration: int, 
                               adults: int, cabin: Optional[str], currency: str, max_stops: Optional[int]) -> Optional[Dict[str,Any]]:
    return_date = depart + timedelta(days=duration)
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": dest,
        "departureDate": iso(depart),
        "returnDate": iso(return_date),
        "adults": str(adults),
        "currencyCode": currency,
        "max": "20",
        "sort": "PRICE"
    }
    if cabin:
        params["travelClass"] = cabin
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(AMAD_SEARCH_URL, headers=headers, params=params, timeout=30)
    if resp.status_code >= 400:
        return None
    data = resp.json()
    offers = data.get("data", [])
    if not offers:
        return None
    if max_stops is not None:
        fil = []
        for o in offers:
            it0 = safe_get(o, ["itineraries", 0, "segments"], [])
            it1 = safe_get(o, ["itineraries", 1, "segments"], [])
            if max(0, len(it0)-1) <= max_stops and max(0, len(it1)-1) <= max_stops:
                fil.append(o)
        offers = fil or offers
    cheapest = pick_cheapest_offer(offers)
    if not cheapest:
        return None
    s = summarize_offer(cheapest)
    s.update({
        "origin": origin, "destination": dest,
        "depart_date": iso(depart),
        "return_date": iso(return_date),
    })
    return s

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
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"<h2>Daily Flight Watcher — {now}</h2>"]
    if not best:
        lines.append("<p>No offers found today.</p>")
    else:
        lines.append("<ul>")
        for r in sorted(best, key=lambda x: (x['route_name'], x['duration_days'])):
            lines.append(
                f"<li><b>{r['route_name']}</b> — {r['duration_days']} days: "
                f"Depart <b>{r['depart_date']}</b>, Return <b>{r['return_date']}</b> — "
                f"{r['airline']} — <b>{r['price']} {r['currency']}</b> "
                f"(stops out/ret: {r['stops_out']}/{r['stops_ret']})</li>"
            )
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
