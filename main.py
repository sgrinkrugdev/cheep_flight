#!/usr/bin/env python3
import os, sys, csv, math, json, requests, traceback
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional
import yaml

AMAD_ENDPOINTS = {
    "test": {
        "auth":   "https://test.api.amadeus.com/v1/security/oauth2/token",
        "search": "https://test.api.amadeus.com/v2/shopping/flight-offers",
    },
    "prod": {
        "auth":   "https://api.amadeus.com/v1/security/oauth2/token",
        "search": "https://api.amadeus.com/v2/shopping/flight-offers",
    },
}

def resolve_amadeus_env(cfg: Dict[str, Any]) -> str:
    env_from_var = (os.getenv("AMADEUS_ENV") or "").strip().lower()
    env_from_cfg = str(cfg.get("amadeus", {}).get("env", "test")).strip().lower()
    env = (env_from_var or env_from_cfg or "test")
    if env not in ("test", "prod"):
        env = "test"
    print(f"[Amadeus] Effective env: {env} (AMADEUS_ENV={env_from_var!r}, config={env_from_cfg!r})")
    return env


def get_amadeus_credentials(cfg, env: str):
    """
    Priority:
      1) Environment-specific GitHub secrets
         - test: AMADEUS_API_KEY_TEST / AMADEUS_API_SECRET_TEST
         - prod: AMADEUS_API_KEY_PROD / AMADEUS_API_SECRET_PROD
      2) Values in config.yaml (amadeus.api_key / amadeus.api_secret)
    """
    if env == "prod":
        key = os.getenv("AMADEUS_API_KEY_PROD")
        sec = os.getenv("AMADEUS_API_SECRET_PROD")
    else:
        key = os.getenv("AMADEUS_API_KEY_TEST") or os.getenv("AMADEUS_API_KEY")
        sec = os.getenv("AMADEUS_API_SECRET_TEST") or os.getenv("AMADEUS_API_SECRET")

    # final fallback to config file (not recommended for prod)
    if not key:
        key = cfg.get("amadeus", {}).get("api_key")
    if not sec:
        sec = cfg.get("amadeus", {}).get("api_secret")

    if not key or not sec:
        raise RuntimeError(
            f"Amadeus credentials missing for env='{env}'. "
            f"Expected GitHub secrets "
            f"{'AMADEUS_API_KEY_PROD/AMADEUS_API_SECRET_PROD' if env=='prod' else 'AMADEUS_API_KEY_TEST/AMADEUS_API_SECRET_TEST'} "
            f"or config.amadeus.api_key/api_secret."
        )
    return key, sec
def _parse_iso(dt_str: str):
    if not dt_str:
        return None
    s = dt_str.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _fmt_day_from_iso_date(s: str) -> str:
    # "2026-07-06" -> "06 Jul"
    try:
        d = date.fromisoformat(s)
        return d.strftime("%d %b")
    except Exception:
        return s or ""

def _fmt_day_from_dt(dt_iso: str) -> str:
    # "2026-07-06T21:05:00-04:00" -> "06 Jul"
    dt = _parse_iso(dt_iso)
    if not dt:
        return ""
    try:
        return dt.strftime("%d %b")
    except ValueError:
        # Windows fallback: keep leading zero if present
        return dt.strftime("%d %b")

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

def amadeus_get_token(auth_url: str, client_id: str, client_secret: str) -> str:
    resp = requests.post(
        auth_url,
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

    itin0 = safe_get(offer, ["itineraries", 0], {}) or {}
    itin1 = safe_get(offer, ["itineraries", 1], {}) or {}

    out_itin_duration = itin0.get("duration", "")  # e.g. "PT8H45M" (door-to-door, includes layovers)
    ret_itin_duration = itin1.get("duration", "")

    def map_segments(itin):
        segs_out = []
        for seg in safe_get(itin, ["segments"], []) or []:
            cc  = str(seg.get("carrierCode", "") or "")
            no  = str(seg.get("number", "") or "")
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
                "duration": seg.get("duration", ""),   # NEW: pure flight-time for this segment (PTxHxM)
            })
        return segs_out

    outbound_segments = map_segments(itin0)
    return_segments  = map_segments(itin1)

    out0 = outbound_segments[0] if outbound_segments else {}
    outN = outbound_segments[-1] if outbound_segments else {}
    ret0 = return_segments[0]  if return_segments  else {}
    retN = return_segments[-1] if return_segments  else {}

    return {
        "price": price,
        "currency": currency,
        "airline": validating,

        # segments
        "outbound_segments": outbound_segments,
        "return_segments": return_segments,

        # quick fields
        "out_depart": out0.get("dep_at", ""),
        "ret_depart": ret0.get("dep_at", ""),
        "out_arrive": outN.get("arr_at", ""),
        "ret_arrive": retN.get("arr_at", ""),
        "stops_out": max(0, len(outbound_segments) - 1),
        "stops_ret": max(0, len(return_segments) - 1),

        # for CSV
        "out_flight": out0.get("flight_number", ""),
        "ret_flight": ret0.get("flight_number", ""),
        "airline_name_out": out0.get("carrier_name", ""),
        "airline_name_ret": ret0.get("carrier_name", ""),

        # NEW: itinerary door-to-door durations
        "out_total_duration": out_itin_duration,   # PT… includes layovers
        "ret_total_duration": ret_itin_duration,
    }


def search_cheapest_for_window(
    token: str,
    search_url: str,
    origin: str,
    dest: str,
    depart: date,
    duration: int,
    adults: int,
    cabin: Optional[str],
    currency: str,
    max_stops: Optional[int],
    max_flight_duration: Optional[float],  # hours, per direction
) -> Optional[Dict[str, Any]]:
    return_date = depart + timedelta(days=duration)
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": dest,
        "departureDate": iso(depart),
        "returnDate": iso(return_date),
        "adults": str(adults),
        "currencyCode": currency,
        "max": "250",
        # no 'sort' in test env
    }
    if cabin:
        params["travelClass"] = cabin

    # If caller wants nonstop, let Amadeus filter at the source.
    if max_stops == 0:
       params["nonStop"] = "true"

    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(search_url, headers=headers, params=params, timeout=30)
    if resp.status_code >= 400:
        return None
    data = resp.json()
    offers = data.get("data", [])
    if not offers:
        return None
    # ---- helpers to compute per-direction totals (prefer itinerary["duration"]) ----
    import re

    _ISO8601_DUR_RE = re.compile(r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?)?$")

    def _minutes_from_iso8601(dur: str) -> Optional[int]:
        """
        Parse 'PT8H45M', 'PT9H', 'P1DT2H', 'PT55M' into total minutes.
        Returns None if not parseable.
        """
        if not isinstance(dur, str):
            return None
        m = _ISO8601_DUR_RE.match(dur)
        if not m:
            return None
        days = int(m.group("days") or 0)
        hours = int(m.group("hours") or 0)
        minutes = int(m.group("minutes") or 0)
        return days * 24 * 60 + hours * 60 + minutes

    def _direction_total_minutes(itin: dict) -> Optional[int]:
        # 1) Prefer Amadeus itinerary door-to-door duration (includes layovers)
        dur_str = (itin or {}).get("duration", "")
        mins = _minutes_from_iso8601(dur_str)
        if mins is not None:
            return mins

        # 2) Fallback: compute from first departure → last arrival timestamps
        segs = (itin or {}).get("segments", []) or []
        if not segs:
            return None
        first_dep = _parse_iso(segs[0].get("departure", {}).get("at", ""))
        last_arr  = _parse_iso(segs[-1].get("arrival", {}).get("at", ""))
        if not first_dep or not last_arr:
            return None
        return int((last_arr - first_dep).total_seconds() // 60)

    def _stops_count(itin: dict) -> int:
        segs = (itin or {}).get("segments", []) or []
        return max(0, len(segs) - 1)



    # ---- apply filters: max_stops and max_flight_duration (per direction) ----
    filtered = []
    for o in offers:
        it0 = o.get("itineraries", [{}])[0]
        it1 = o.get("itineraries", [{}])[1] if len(o.get("itineraries", [])) > 1 else {}

        # stops filter
        if max_stops is not None:
            if _stops_count(it0) > max_stops or _stops_count(it1) > max_stops:
                continue

        # duration per direction (door-to-door, includes layovers & tz offsets)
        if max_flight_duration is not None:
            cap_mins = int(float(max_flight_duration) * 60)
            t0 = _direction_total_minutes(it0)
            t1 = _direction_total_minutes(it1)
            if (t0 is not None and t0 > cap_mins) or (t1 is not None and t1 > cap_mins):
                continue

        filtered.append(o)

    if not filtered:
        return None
    offers = filtered

    # choose cheapest remaining offer
    cheapest = pick_cheapest_offer(offers)
    if not cheapest:
        return None

    carriers = {}  # (optional map)
    s = summarize_offer(cheapest, carriers)
    s.update({
        "origin": origin, "destination": dest,
        "depart_date": iso(depart),
        "return_date": iso(return_date),
    })
    s["cap_max_stops"] = max_stops
    s["cap_max_flight_duration"] = max_flight_duration
    return s


def run_search(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    env = resolve_amadeus_env(cfg)
    endpoints = AMAD_ENDPOINTS[env]
    ci, cs = get_amadeus_credentials(cfg, env)

    # NEW: log what we’re about to use
    print(f"[Amadeus] Using env={env} auth={endpoints['auth']} search={endpoints['search']}")
    print(f"[Amadeus] Credentials source: {'PROD' if env=='prod' else 'TEST'} GitHub Secrets")

    token = amadeus_get_token(endpoints["auth"], ci, cs)


    currency = cfg.get("currency", "USD")
    adults = int(cfg.get("adults", 1))
    cabin = cfg.get("cabin")

    # global defaults (can be None)
    global_max_stops = cfg.get("max_stops", None)
    global_max_stops = int(global_max_stops) if global_max_stops is not None else None
    global_max_fd = cfg.get("max_flight_duration", None)  # hours; per direction
    global_max_fd = float(global_max_fd) if global_max_fd is not None else None

    results = []
    for route in cfg["routes"]:
        origin = route["origin"]
        dest = route["destination"]

        # per-route overrides with fallback to globals
        route_max_stops = route.get("max_stops", global_max_stops)
        route_max_stops = int(route_max_stops) if route_max_stops is not None else None

        route_max_fd = route.get("max_flight_duration", global_max_fd)  # hours
        route_max_fd = float(route_max_fd) if route_max_fd is not None else None

        # dates
        start_raw = route["start_date"]
        end_raw = route["end_date"]
        start = date.fromisoformat(str(start_raw))
        end = date.fromisoformat(str(end_raw))

        durations = route.get("durations", [10])
        for dur in durations:
            dur = int(dur)
            for d0 in daterange(start, end - timedelta(days=dur)):
                try:
                    found = search_cheapest_for_window(
                        token=token,
                        search_url=endpoints["search"],
                        origin=origin,
                        dest=dest,
                        depart=d0,
                        duration=dur,
                        adults=adults,
                        cabin=cabin,
                        currency=currency,
                        max_stops=route_max_stops,
                        max_flight_duration=route_max_fd,
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
            f.write(
                "run_ts,route_name,origin,destination,depart_date,return_date,"
                "duration_days,airline,price,currency,stops_out,stops_ret,"
                "out_depart,ret_depart,out_flight,ret_flight\n"
            )

def append_log(path: str, rows: List[Dict[str, Any]]):
    ensure_log(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        for r in rows:
            f.write(",".join([
                datetime.utcnow().isoformat(),
                r.get("route_name",""), r.get("origin",""), r.get("destination",""),
                r.get("depart_date",""), r.get("return_date",""),
                str(r.get("duration_days","")), r.get("airline",""), str(r.get("price","")), r.get("currency",""),
                str(r.get("stops_out","")), str(r.get("stops_ret","")),
                r.get("out_depart",""), r.get("ret_depart",""),
                r.get("out_flight",""), r.get("ret_flight",""),
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

def send_email_ipower(subject: str, html: str, to_email: str, from_email: str,
                      host: str, port: int, username: str, password: str):
    import smtplib, ssl
    from email.mime.text import MIMEText

    msg = MIMEText(html, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(host=host, port=int(port), context=context, timeout=30) as server:
        server.login(username, password)
        server.sendmail(from_email, [to_email], msg.as_string())

def build_daily_digest(best, cfg):
    """
    Renders email:
      • If results exist for a route+duration: show full details (segments, totals, footer).
      • If no results for a route+duration: show "No Flights" and the filters footer.
    Dates in headers are formatted as "DD Mon".
    """

    from datetime import datetime as _dt, timedelta

    # ---------- helpers ----------
    def _parse_iso(dt_str):
        if not dt_str:
            return None
        s = dt_str.replace("Z", "+00:00")
        try:
            return _dt.fromisoformat(s)
        except Exception:
            return None

    def _fmt_date_short(d_str: str) -> str:
        # Input "YYYY-MM-DD" (string) -> "DD Mon"
        try:
            d = _dt.fromisoformat(str(d_str))
        except Exception:
            try:
                d = _dt.strptime(str(d_str), "%Y-%m-%d")
            except Exception:
                return str(d_str)
        return d.strftime("%d %b")

    def _fmt_dt_long(dt_str):
        dt = _parse_iso(dt_str)
        if not dt:
            return ""
        try:
            return dt.strftime("%-d %b · %-I:%M %p")
        except ValueError:
            return dt.strftime("%d %b · %I:%M %p").lstrip("0").replace(" 0", " ")

    def _plus_days(a_str, b_str):
        a, b = _parse_iso(a_str), _parse_iso(b_str)
        if not a or not b: return ""
        d = (b.date() - a.date()).days
        return f"+{d}" if d > 0 else ""

    def _parse_iso8601_duration(dur: str):
        if not dur or not isinstance(dur, str) or not dur.startswith("P"):
            return None
        days = hours = minutes = 0
        try:
            date_part, time_part = dur, ""
            if "T" in dur:
                date_part, time_part = dur.split("T", 1)
            if "D" in date_part:
                idx = date_part.index("D")
                days = int(date_part[1:idx] or 0)
            if "H" in time_part:
                num = "".join(ch for ch in time_part.split("H")[0] if ch.isdigit())
                hours = int(num or 0)
                time_part = time_part.split("H", 1)[1]
            if "M" in time_part:
                num = "".join(ch for ch in time_part.split("M")[0] if ch.isdigit())
                minutes = int(num or 0)
            return timedelta(days=days, hours=hours, minutes=minutes)
        except Exception:
            return None

    def _dur_td(start_str, end_str):
        a, b = _parse_iso(start_str), _parse_iso(end_str)
        if not a or not b: return None
        return b - a

    def _fmt_td(td):
        if td is None: return ""
        total_mins = int(td.total_seconds() // 60)
        hrs, mins = divmod(total_mins, 60)
        if hrs and mins: return f"{hrs} hr {mins} min"
        if hrs: return f"{hrs} hr"
        return f"{mins} min"

    def _fmt_hours(h):
        if h is None: return None
        try:
            hf = float(h)
            return f"{int(hf)} hr" if hf.is_integer() else f"{hf:.1f} hr"
        except Exception:
            return f"{h} hr"

    # Build a map of found results by (route_name, duration_days)
    found_map = {}
    for r in best or []:
        key = (r.get("route_name", ""), int(r.get("duration_days", 0)))
        found_map[key] = r

    #fixing the issue of local time vs UTC and PROD/TEST indicator
    #now = datetime.now().strftime("%Y-%m-%d %H:%M")
    #lines = [f"<h2>Daily Flight Watcher — {now}</h2>"]

    from datetime import datetime
    import pytz

    # Local timezone conversion
    local_tz = pytz.timezone("America/New_York")
    local_time = datetime.now(local_tz)
    time_str = local_time.strftime("%Y-%m-%d %H:%M %Z")

    # Add TEST marker if applicable
    env_marker = " TEST" if env == "test" else ""

    lines = [f"<h2>Daily Flight Watcher — {time_str}{env_marker}</h2>"]


    routes = cfg.get("routes", [])
    # Global defaults
    g_max_stops = cfg.get("max_stops", None)
    g_max_stops = int(g_max_stops) if g_max_stops is not None else None
    g_max_fd = cfg.get("max_flight_duration", None)
    g_max_fd = float(g_max_fd) if g_max_fd is not None else None

    # Iterate in config order to ensure "No Flights" entries are shown too
    for route in routes:
        origin = route.get("origin", "")
        dest   = route.get("destination", "")
        name   = route.get("name", f"{origin}-{dest}")

        # caps (per-route overrides)
        cap_stops = route.get("max_stops", g_max_stops)
        cap_stops = int(cap_stops) if cap_stops is not None else None
        cap_fd    = route.get("max_flight_duration", g_max_fd)
        cap_fd    = float(cap_fd) if cap_fd is not None else None

        # dates/durations from config
        start_s = str(route.get("start_date", ""))
        end_s   = str(route.get("end_date", ""))
        start_fmt = _fmt_date_short(start_s)
        end_fmt   = _fmt_date_short(end_s)
        durations = route.get("durations", [10])

        for dur in durations:
            key = (name, int(dur))
            r = found_map.get(key)

            if not r:
                # --- No Flights block ---
                header = f"{origin} to {dest} No Flights {start_fmt} – {end_fmt}"
                lines.append(f"<div style='margin:0'><b>{header}</b></div>")
                cap_stops_txt = (str(cap_stops) if cap_stops is not None else "any")
                cap_fd_txt    = _fmt_hours(cap_fd) if cap_fd is not None else "no limit"
                lines.append(
                    f"<div style='color:#555'>Filters: Stops ≤ {cap_stops_txt}, "
                    f"Per-direction travel time ≤ {cap_fd_txt}</div>"
                )
                lines.append("<div style='margin:10px 0 16px 0;'></div>")
                continue

            # ---------- Have a result: render detailed block ----------
            out = r.get("outbound_segments", []) or []
            ret = r.get("return_segments", []) or []
            
            # Actual trip dates from chosen itinerary
            trip_start_dt = (out[0]["dep_at"] if out else r.get("out_depart"))
            trip_end_dt   = (ret[-1]["arr_at"] if ret else (out[-1]["arr_at"] if out else r.get("ret_arrive")))
            trip_start_txt = _fmt_day_from_dt(trip_start_dt)
            trip_end_txt   = _fmt_day_from_dt(trip_end_dt)
            
            search_start_txt = _fmt_day_from_iso_date(r.get("depart_date",""))
            search_end_txt   = _fmt_day_from_iso_date(r.get("return_date",""))

            # Prefer itinerary durations if provided
            out_total_td = _parse_iso8601_duration(r.get("out_total_duration", "")) or (
                _dur_td(out[0]["dep_at"], out[-1]["arr_at"]) if out else None
            )
            ret_total_td = _parse_iso8601_duration(r.get("ret_total_duration", "")) or (
                _dur_td(ret[0]["dep_at"], ret[-1]["arr_at"]) if ret else None
            )

            # max travel time across directions
            max_td = None
            if out_total_td and ret_total_td:
                max_td = max(out_total_td, ret_total_td, key=lambda t: t.total_seconds())
            else:
                max_td = out_total_td or ret_total_td
            


            # Header with new date format
            header_bits = [
            f"{r.get('origin','')} to {r.get('destination','')}",
            f"${r.get('price', 0):.2f} {r.get('currency','')}",
            f"{trip_start_txt} – {trip_end_txt}",
            ]
            if max_td:
                header_bits.append(f"Max travel time { _fmt_td(max_td) }")

            lines.append(f"<div style='margin:0'><b>{' '.join(header_bits)}</b></div>")


            # Outbound
            stops_out = max(0, len(out) - 1)
            lines.append(
                f"<div><i>Outbound</i> "
                f"{('Travel time ' + _fmt_td(out_total_td)) if out_total_td else ''}"
                f"{(' ' if out_total_td else '')}{stops_out} stop{'s' if stops_out!=1 else ''}</div>"
            )
            if not out:
                lines.append("<div>— (no outbound segments found)</div>")
            else:
                for seg in out:
                    seg_td = _parse_iso8601_duration(seg.get("duration", "")) or _dur_td(seg.get("dep_at",""), seg.get("arr_at",""))
                    dep_fmt = _fmt_dt_long(seg.get("dep_at", ""))
                    arr_fmt = _fmt_dt_long(seg.get("arr_at", ""))
                    plus_txt = _plus_days(seg.get("dep_at",""), seg.get("arr_at",""))

                    carrier_name = (seg.get("carrier_name") or "").strip()
                    carrier_code = (seg.get("carrier_code") or "").strip()
                    flight_no    = (seg.get("flight_number") or "").strip()
                    label = flight_no if (not carrier_name or carrier_name.upper() == carrier_code.upper()) else f"{carrier_name.upper()} {flight_no}"

                    lines.append(
                        "<div>"
                        f"{label} {dep_fmt}–{arr_fmt}{plus_txt} "
                        f"{seg.get('dep_airport','')}–{seg.get('arr_airport','')} "
                        f"Flight time {_fmt_td(seg_td)}"
                        "</div>"
                    )

            # Return
            stops_ret = max(0, len(ret) - 1)
            lines.append(
                f"<div><i>Return</i> "
                f"{('Travel time ' + _fmt_td(ret_total_td)) if ret_total_td else ''}"
                f"{(' ' if ret_total_td else '')}{stops_ret} stop{'s' if stops_ret!=1 else ''}</div>"
            )
            if not ret:
                lines.append("<div>— (no return segments found)</div>")
            else:
                for seg in ret:
                    seg_td = _parse_iso8601_duration(seg.get("duration", "")) or _dur_td(seg.get("dep_at",""), seg.get("arr_at",""))
                    dep_fmt = _fmt_dt_long(seg.get("dep_at", ""))
                    arr_fmt = _fmt_dt_long(seg.get("arr_at", ""))
                    plus_txt = _plus_days(seg.get("dep_at",""), seg.get("arr_at",""))

                    carrier_name = (seg.get("carrier_name") or "").strip()
                    carrier_code = (seg.get("carrier_code") or "").strip()
                    flight_no    = (seg.get("flight_number") or "").strip()
                    label = flight_no if (not carrier_name or carrier_name.upper() == carrier_code.upper()) else f"{carrier_name.upper()} {flight_no}"

                    lines.append(
                        "<div>"
                        f"{label} {dep_fmt}–{arr_fmt}{plus_txt} "
                        f"{seg.get('dep_airport','')}–{seg.get('arr_airport','')} "
                        f"Flight time {_fmt_td(seg_td)}"
                        "</div>"
                    )

                # --- footer for this itinerary (filters + CONFIG search window) ---
                max_stops_cap = r.get("cap_max_stops", None)
                max_fd_cap    = r.get("cap_max_flight_duration", None)  # hours per direction
                cap_stops_txt = f"Stops ≤ {max_stops_cap}" if max_stops_cap is not None else "Stops: any"
                cap_fd_txt    = (
                    f"Per-direction travel time ≤ {int(max_fd_cap)} hr"
                    if max_fd_cap is not None else "Per-direction travel time: any"
                )
                lines.append(
                    f"<div>Filters: {cap_stops_txt}, {cap_fd_txt} — "
                    f"Window: {start_fmt} – {end_fmt}</div>"
                )
                lines.append("<div style='margin:10px 0 16px 0;'></div>")

            

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

    email_cfg = cfg.get("email", {})
    provider = (email_cfg.get("provider") or "sendgrid").strip().lower()
    to_email = os.getenv("TO_EMAIL") or email_cfg["to"]
    from_email = os.getenv("FROM_EMAIL") or email_cfg["from"]

    if provider in ("sendgrid", "sg"):
        send_email_sendgrid(subj, html, to_email, from_email)
    elif provider in ("ipower", "smtp"):
        host = os.getenv("SMTP_HOST") or email_cfg.get("smtp_host", "smtp.ipower.com")
        port = os.getenv("SMTP_PORT") or email_cfg.get("smtp_port", 465)
        user = os.getenv("SMTP_USERNAME")
        pwd  = os.getenv("SMTP_PASSWORD")
        if not user or not pwd:
            raise RuntimeError("SMTP_USERNAME / SMTP_PASSWORD not set (use GitHub Secrets).")
        send_email_ipower(subj, html, to_email, from_email, host, port, user, pwd)
    else:
        raise RuntimeError(f"Unknown email provider: {provider}")

    print("Email sent via", provider, "Entries:", len(best))




if __name__ == "__main__":
    main()
