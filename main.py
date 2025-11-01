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
        "airline": validating,  # validating airline code

        # segments
        "outbound_segments": outbound_segments,
        "return_segments": return_segments,

        # quick fields used elsewhere
        "out_depart": out0.get("dep_at", ""),
        "ret_depart": ret0.get("dep_at", ""),
        "out_arrive": outN.get("arr_at", ""),
        "ret_arrive": retN.get("arr_at", ""),
        "stops_out": max(0, len(outbound_segments) - 1),
        "stops_ret": max(0, len(return_segments) - 1),

        # new fields you need for the CSV
        "out_flight": out0.get("flight_number", ""),
        "ret_flight": ret0.get("flight_number", ""),
        "airline_name_out": out0.get("carrier_name", ""),
        "airline_name_ret": ret0.get("carrier_name", ""),
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
    Email renderer with per-segment durations, per-direction totals, stop counts,
    and overall Max travel time (max of outbound/return direction totals).
    Expects summarize_offer() to provide:
      - outbound_segments / return_segments: list of dicts with:
          carrier_name, flight_number, dep_airport, arr_airport, dep_at, arr_at
      - origin, destination, price, currency, depart_date, return_date
      - stops_out, stops_ret, duration_days, route_name
    """

    def _parse_iso(dt_str):
        if not dt_str:
            return None
        s = dt_str.replace("Z", "+00:00")
        try:
            # Handles timezone-aware strings like 2025-12-01T20:30:00-05:00
            return datetime.fromisoformat(s)
        except Exception:
            return None

    def _fmt_dt(dt_str):
        dt = _parse_iso(dt_str)
        if not dt:
            return ""
        # Example: "6 Jul · 9:05 PM"
        # Use %-d / %-I on Linux; on Windows you may want %#d / %#I
        try:
            return dt.strftime("%-d %b · %-I:%M %p")
        except ValueError:
            return dt.strftime("%d %b · %I:%M %p").lstrip("0").replace(" 0", " ")

    def _fmt_dt_long(dt_str):
        dt = _parse_iso(dt_str)
        if not dt:
            return ""
        # Example: "7 Jul · 9:20 AM"
        try:
            return dt.strftime("%-d %b · %-I:%M %p")
        except ValueError:
            return dt.strftime("%d %b · %I:%M %p").lstrip("0").replace(" 0", " ")

    def _plus_days(a_str, b_str):
        a, b = _parse_iso(a_str), _parse_iso(b_str)
        if not a or not b:
            return ""
        delta_days = (b.date() - a.date()).days
        return (f"+{delta_days}" if delta_days > 0 else "")

    def _dur_td(start_str, end_str):
        a, b = _parse_iso(start_str), _parse_iso(end_str)
        if not a or not b:
            return None
        return b - a  # includes timezone offset correctly

    def _fmt_td(td):
        if td is None:
            return ""
        total_mins = int(td.total_seconds() // 60)
        hrs, mins = divmod(total_mins, 60)
        if hrs and mins:
            return f"{hrs} hr {mins} min"
        if hrs:
            return f"{hrs} hr"
        return f"{mins} min"

    def _direction_total(segs):
        """Total travel time for a direction: first departure → last arrival (includes layovers)."""
        if not segs:
            return None
        first_dep = segs[0].get("dep_at")
        last_arr = segs[-1].get("arr_at")
        return _dur_td(first_dep, last_arr)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"<h2>Daily Flight Watcher — {now}</h2>"]

    if not best:
        lines.append("<p>No offers found today.</p>")
        return "\n".join(lines)

    best_sorted = sorted(best, key=lambda x: (x.get('route_name', ''), x.get('duration_days', 0)))

    for r in best_sorted:
        out = r.get("outbound_segments", []) or []
        ret = r.get("return_segments", []) or []

        # per-direction totals
        out_total = _direction_total(out)
        ret_total = _direction_total(ret)

        # max travel time across directions
        max_td = None
        if out_total and ret_total:
            max_td = max(out_total, ret_total, key=lambda t: t.total_seconds())
        else:
            max_td = out_total or ret_total

        # header
        header_bits = [
            f"{r.get('origin','')} to {r.get('destination','')}",
            f"${r.get('price', 0):.2f} {r.get('currency','')}",
            f"{r.get('depart_date','')} – {r.get('return_date','')}",
        ]
        if max_td:
            header_bits.append(f"Max travel time { _fmt_td(max_td) }")

        lines.append(f"<p><b>{' '.join(header_bits)}</b></p>")

        # --- Outbound ---
        stops_out = max(0, len(out) - 1)
        out_total_txt = _fmt_td(out_total) if out_total else ""
        lines.append(
            f"<div><i>Outbound</i> "
            f"{('Travel time ' + out_total_txt) if out_total_txt else ''}"
            f"{(' ' if out_total_txt else '')}{stops_out} stop{'s' if stops_out!=1 else ''}</div>"
        )

        if not out:
            lines.append("<div>— (no outbound segments found)</div>")
        else:
            for seg in out:
                dep_s = seg.get("dep_at", "")
                arr_s = seg.get("arr_at", "")
                seg_td = _dur_td(dep_s, arr_s)
                seg_td_txt = _fmt_td(seg_td)

                dep_fmt = _fmt_dt_long(dep_s)
                arr_fmt = _fmt_dt_long(arr_s)
                plus = _plus_days(dep_s, arr_s)
                plus_txt = f"{plus}" if plus else ""

                lines.append(
                    "<div>"
                    f"{seg.get('carrier_name','').upper()} {seg.get('flight_number','')} "
                    f"{dep_fmt}–{arr_fmt}{plus_txt} "
                    f"{seg.get('dep_airport','')}–{seg.get('arr_airport','')} "
                    f"Flight time {seg_td_txt}"
                    "</div>"
                )

        # --- Return ---
        stops_ret = max(0, len(ret) - 1)
        ret_total_txt = _fmt_td(ret_total) if ret_total else ""
        lines.append(
            f"<div><i>Return</i> "
            f"{('Travel time ' + ret_total_txt) if ret_total_txt else ''}"
            f"{(' ' if ret_total_txt else '')}{stops_ret} stop{'s' if stops_ret!=1 else ''}</div>"
        )

        if not ret:
            lines.append("<div>— (no return segments found)</div>")
        else:
            for seg in ret:
                dep_s = seg.get("dep_at", "")
                arr_s = seg.get("arr_at", "")
                seg_td = _dur_td(dep_s, arr_s)
                seg_td_txt = _fmt_td(seg_td)

                dep_fmt = _fmt_dt_long(dep_s)
                arr_fmt = _fmt_dt_long(arr_s)
                plus = _plus_days(dep_s, arr_s)
                plus_txt = f"{plus}" if plus else ""

                lines.append(
                    "<div>"
                    f"{seg.get('carrier_name','').upper()} {seg.get('flight_number','')} "
                    f"{dep_fmt}–{arr_fmt}{plus_txt} "
                    f"{seg.get('dep_airport','')}–{seg.get('arr_airport','')} "
                    f"Flight time {seg_td_txt}"
                    "</div>"
                )

        # You can keep your search-parameter footer if desired; omitted here for brevity.
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
