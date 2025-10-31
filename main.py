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
                               adults: int, cabin: Optional[str], currency: str,
                               max_stops: Optional[int], max_flight_duration_hours: Optional[int]) -> Optional[Dict[str,Any]]:
    """
    Query Amadeus for a single (origin, dest, depart, return) window and return the cheapest offer
    after applying:
      - max_stops: maximum stops per direction (itinerary), if provided
      - max_flight_duration_hours: maximum total air time per direction (itinerary), if provided
        (i.e., outbound itinerary duration <= threshold AND return itinerary duration <= threshold)
    """
    from time import sleep

    return_date = depart + timedelta(days=duration)
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": dest,
        "departureDate": depart.isoformat(),
        "returnDate": return_date.isoformat(),
        "adults": str(adults),
        "currencyCode": currency,
        "max": "20"  # keep; don't use 'sort' on test env
    }
    if cabin:
        params["travelClass"] = cabin  # ECONOMY, PREMIUM_ECONOMY, BUSINESS, FIRST

    headers = {"Authorization": f"Bearer {token}"}

    # --- simple retry for transient 429s ---
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
            snippet = resp.text[:400].replace("\n", " ")
            print(f"[debug] {origin}->{dest} {depart} dur={duration}: HTTP {resp.status_code} {snippet}")
            return None

        data = resp.json()
        offers = data.get("data", []) or []
        carriers = data.get("dictionaries", {}).get("carriers", {})

        if not offers:
            print(f"[debug] {origin}->{dest} {depart} dur={duration}: offers=0")
            return None

        # --- helpers for filtering ---
        def itinerary_minutes(itin: dict) -> int:
            segs = itin.get("segments", []) or []
            if not segs:
                return 0
            def _p(s: str) -> datetime:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            dep_ts = _p(segs[0]["departure"]["at"])
            arr_ts = _p(segs[-1]["arrival"]["at"])
            return int((arr_ts - dep_ts).total_seconds() // 60)

        filtered = []
        for o in offers:
            it_out = safe_get(o, ["itineraries", 0], {}) or {}
            it_ret = safe_get(o, ["itineraries", 1], {}) or {}
            seg_out = it_out.get("segments", []) or []
            seg_ret = it_ret.get("segments", []) or []

            # max_stops filter (per direction)
            if max_stops is not None:
                stops_out = max(0, len(seg_out) - 1)
                stops_ret = max(0, len(seg_ret) - 1)
                if stops_out > max_stops or stops_ret > max_stops:
                    continue

            # max_flight_duration_hours filter (per direction)
            if max_flight_duration_hours is not None:
                limit_mins = int(max_flight_duration_hours) * 60
                out_mins = itinerary_minutes(it_out)
                ret_mins = itinerary_minutes(it_ret)
                # Enforce per-direction cap (both must comply)
                if out_mins > limit_mins or ret_mins > limit_mins:
                    continue

            filtered.append(o)

        # If all offers were filtered out, return None (strict enforcement)
        if not filtered:
            print(f"[debug] {origin}->{dest} {depart} dur={duration}: offers={len(offers)} filtered=0")
            return None

        cheapest = pick_cheapest_offer(filtered)
        if not cheapest:
            print(f"[debug] {origin}->{dest} {depart} dur={duration}: cheapest=n/a after filtering")
            return None

        summary = summarize_offer(cheapest, carriers)
        summary.update({
            "origin": origin,
            "destination": dest,
            "depart_date": depart.isoformat(),
            "return_date": return_date.isoformat(),
        })
        return summary

    # Retries exhausted
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
    max_flight_duration = cfg.get("max_flight_duration", None)
    max_flight_duration = int(max_flight_duration) if max_flight_duration is not None else None


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
                        token, origin, dest, d0, dur, adults, cabin, currency, max_stops, max_flight_duration
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
    """
    Compact digest email:
      1) <b><DEP> to <ARR> $PRICE CUR START – END, Total/Max Travel Time X hr Y min</b>
      2) OUTBOUND single line
      3) RETURN   single line
      Footer: Stops / Max Flight Duration / window
      Also emits 'no flights found' blocks for route+duration combos with zero results.
    """
    # ---------- helpers ----------
    def _parse_dt(dt):
        if not dt:
            return None
        try:
            return datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
        except Exception:
            return None

    def _day_label(dt):       # "4 Dec"
        return dt.strftime("%-d %b") if dt else ""

    def _time_label(dt):      # "8:30 PM"
        return dt.strftime("%-I:%M %p") if dt else ""

    def _next_day(dep, arr):  # "+1" if arrival date > departure date
        if not dep or not arr:
            return ""
        return "+1" if arr.date() > dep.date() else ""

    def _stops_label(n):
        return "Nonstop" if n <= 0 else ("1 stop" if n == 1 else f"{n} stops")

    def _first(segs): return segs[0] if segs else {}
    def _last(segs):  return segs[-1] if segs else {}

    def _price_fmt(val, curr):
        try:
            return f"${float(val):,.2f} {curr}".strip()
        except Exception:
            return f"{val} {curr}".strip()

    def _dur_label_mins(mins):
        h, m = mins // 60, mins % 60
        if h and m: return f"{h} hr {m} min"
        if h:       return f"{h} hr"
        return f"{m} min"

    def _itinerary_minutes_from_segments(segs):
        """
        Your mapped segments have:
          dep_at / arr_at (ISO strings with or without TZ)
        Compute elapsed time (incl. layovers) = first dep -> last arr.
        """
        if not segs:
            return 0
        dep_dt = _parse_dt(segs[0].get("dep_at") or segs[0].get("departure", {}).get("at"))
        arr_dt = _parse_dt(segs[-1].get("arr_at") or segs[-1].get("arrival", {}).get("at"))
        if not dep_dt or not arr_dt:
            return 0
        return int((arr_dt - dep_dt).total_seconds() // 60)

    # ---------- prep ----------
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"<h2>Daily Flight Watcher — {now}</h2>"]

    # Index winners by (origin, destination, duration_days)
    found_index = {}
    for r in best or []:
        key = (r.get("origin",""), r.get("destination",""), int(r.get("duration_days", 0)))
        found_index[key] = r

    # Global constraints for footer display
    cfg_max_stops = cfg.get("max_stops", None)
    stops_disp = "Any" if cfg_max_stops is None else str(cfg_max_stops)
    mfd_cfg = cfg.get("max_flight_duration", None)
    mfd_disp = "No limit" if mfd_cfg is None else (f"{int(mfd_cfg)} hours" if float(mfd_cfg).is_integer() else f"{float(mfd_cfg):.1f} hours")

    # Render FOUND winners
    for r in sorted(best or [], key=lambda x: (x.get("route_name",""), x.get("duration_days", 0))):
        outs = r.get("outbound_segments", [])
        rets = r.get("return_segments", [])
        o0, oN = _first(outs), _last(outs)
        r0, rN = _first(rets), _last(rets)

        # Per-direction elapsed durations (incl. layovers), TZ-aware
        out_mins = _itinerary_minutes_from_segments(outs)
        ret_mins = _itinerary_minutes_from_segments(rets)

        # Pick header number: maximum of available legs
        legs = [m for m in (out_mins, ret_mins) if m > 0]
        header_tail = ""
        if legs:
            label = "Total Travel Time" if (out_mins and ret_mins) else "Max Travel Time"
            header_tail = f", {label} {_dur_label_mins(max(legs))}"

        dep_air = o0.get("dep_airport","") or r0.get("dep_airport","") or r.get("origin","")
        arr_air = oN.get("arr_airport","")  or rN.get("arr_airport","")  or r.get("destination","")
        price   = _price_fmt(r.get("price",""), r.get("currency",""))
        start_d = r.get("depart_date","")
        end_d   = r.get("return_date","")

        # Bold header
        lines.append(f"<div><b>{dep_air} to {arr_air} {price} {start_d} – {end_d}{header_tail}</b></div>")

        # Parse times for display lines
        o_dep_dt = _parse_dt(o0.get("dep_at") or o0.get("departure", {}).get("at"))
        o_arr_dt = _parse_dt(oN.get("arr_at") or oN.get("arrival",   {}).get("at"))
        r_dep_dt = _parse_dt(r0.get("dep_at") or r0.get("departure", {}).get("at"))
        r_arr_dt = _parse_dt(rN.get("arr_at") or rN.get("arrival",   {}).get("at"))

        # Outbound single line
        if outs:
            o_line = (
                f"{o0.get('carrier_name','')} {o0.get('flight_number','')} "
                f"{_day_label(o_dep_dt)} · {_time_label(o_dep_dt)}–{_time_label(o_arr_dt)}{_next_day(o_dep_dt,o_arr_dt)}"
                f"&nbsp;&nbsp; {_dur_label_mins(out_mins)} "
                f"{o0.get('dep_airport','')}–{oN.get('arr_airport','')}"
                f"&nbsp;&nbsp; {_stops_label(len(outs)-1)}"
            )
            lines.append(f"<div>{o_line}</div>")

        # Return single line
        if rets:
            r_line = (
                f"{r0.get('carrier_name','')} {r0.get('flight_number','')} "
                f"{_day_label(r_dep_dt)} · {_time_label(r_dep_dt)}–{_time_label(r_arr_dt)}{_next_day(r_dep_dt,r_arr_dt)}"
                f"&nbsp;&nbsp; {_dur_label_mins(ret_mins)} "
                f"{r0.get('dep_airport','')}–{rN.get('arr_airport','')}"
                f"&nbsp;&nbsp; {_stops_label(len(rets)-1)}"
            )
            lines.append(f"<div>{r_line}</div>")

        # Footer (per result)
        trip_dur = r.get("duration_days", "")
        footer1 = f"Stops: {stops_disp}, Max Flight Duration: {mfd_disp}"
        footer2 = f"Flying from {dep_air} to {arr_air} for {trip_dur} days between {start_d} and {end_d}"
        lines.append(f"<div style='color:#555'>{footer1}</div>")
        lines.append(f"<div style='color:#555'>{footer2}</div>")
        lines.append("<br>")

    # Render 'no flights found' blocks for route/duration pairs with zero results
    for route in cfg.get("routes", []):
        origin = route["origin"]
        dest = route["destination"]
        start_cfg = route.get("start_date")
        durations = route.get("durations", [10])

        for dur in durations:
            key = (origin, dest, int(dur))
            if key in found_index:
                continue
            # earliest candidate window label: start_date .. start_date + duration
            try:
                s0 = datetime.fromisoformat(str(start_cfg))
                start_label = s0.date().isoformat()
                end_window = (s0 + timedelta(days=int(dur))).date().isoformat()
            except Exception:
                start_label = str(start_cfg)
                end_window = str(start_cfg)

            footer1 = f"Stops: {stops_disp}, Max Flight Duration: {mfd_disp}"
            footer2 = f"Flying from {origin} to {dest} for {int(dur)} days between {start_label} and {end_window}"
            lines.append(f"<div><b>{origin} to {dest} no flights found with these search parameters:</b></div>")
            lines.append(f"<div style='color:#555'>{footer1}</div>")
            lines.append(f"<div style='color:#555'>{footer2}</div>")
            lines.append("<br>")

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
