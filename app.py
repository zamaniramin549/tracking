import json
import os
import secrets
import sqlite3
import string
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from flask import Flask, g, jsonify, redirect, render_template, request

app = Flask(__name__)
DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "openshort.db")
SLUG_ALPHABET = string.ascii_lowercase + string.digits
SLUG_LEN = 8
FINISH_TOKEN_TTL = timedelta(minutes=15)
IPAPI_TIMEOUT_S = 3
# Display times in Malaysia / Kuala Lumpur (UTC+8, no DST).
DISPLAY_TZ = timezone(timedelta(hours=8))
DISPLAY_TZ_LABEL = "MYT"


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(_):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def _migrate_visits(db):
    cols = {row[1] for row in db.execute("PRAGMA table_info(visits)")}
    for col, typ in (
        ("ip_country", "TEXT"),
        ("ip_region", "TEXT"),
        ("ip_city", "TEXT"),
        ("ip_lat", "REAL"),
        ("ip_lon", "REAL"),
        ("gps_lat", "REAL"),
        ("gps_lon", "REAL"),
        ("gps_accuracy_m", "REAL"),
    ):
        if col not in cols:
            db.execute(f"ALTER TABLE visits ADD COLUMN {col} {typ}")


def init_db():
    db = sqlite3.connect(DATABASE)
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            destination_url TEXT NOT NULL,
            stats_token TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS visits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link_id INTEGER NOT NULL REFERENCES links(id),
            ip TEXT,
            user_agent TEXT,
            referrer TEXT,
            visited_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_visits_link ON visits(link_id);
        CREATE TABLE IF NOT EXISTS visit_finish_tokens (
            token TEXT PRIMARY KEY,
            visit_id INTEGER NOT NULL REFERENCES visits(id),
            expires_at TEXT NOT NULL
        );
        """
    )
    _migrate_visits(db)
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS visit_location_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            visit_id INTEGER NOT NULL REFERENCES visits(id),
            lat REAL NOT NULL,
            lng REAL NOT NULL,
            accuracy REAL,
            recorded_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_vlp_visit ON visit_location_points(visit_id);
        """
    )
    db.commit()
    db.close()


def allowed_url(url: str) -> bool:
    try:
        p = urlparse(url.strip())
    except Exception:
        return False
    return p.scheme in ("http", "https") and bool(p.netloc)


def random_slug():
    for _ in range(20):
        s = "".join(secrets.choice(SLUG_ALPHABET) for _ in range(SLUG_LEN))
        db = get_db()
        if db.execute("SELECT 1 FROM links WHERE slug = ?", (s,)).fetchone() is None:
            return s
    raise RuntimeError("Could not allocate slug")


def is_public_ipv4(ip: str) -> bool:
    if not ip or ip == "::1":
        return False
    parts = ip.split(".")
    if len(parts) != 4:
        return True
    try:
        a, b, c, d = (int(x) for x in parts)
    except ValueError:
        return False
    if a == 10:
        return False
    if a == 172 and 16 <= b <= 31:
        return False
    if a == 192 and b == 168:
        return False
    if a == 127:
        return False
    if a == 169 and b == 254:
        return False
    return True


def lookup_ip_geo(ip: str):
    if not is_public_ipv4(ip):
        return {}
    try:
        req = Request(
            f"https://ipapi.co/{ip}/json/",
            headers={"User-Agent": "OpenShort/1.0 (educational)"},
        )
        with urlopen(req, timeout=IPAPI_TIMEOUT_S) as resp:
            data = json.loads(resp.read().decode())
        if data.get("error"):
            return {}
        lat, lon = data.get("latitude"), data.get("longitude")
        return {
            "country": data.get("country_name") or data.get("country_code"),
            "region": data.get("region"),
            "city": data.get("city"),
            "lat": float(lat) if lat is not None else None,
            "lon": float(lon) if lon is not None else None,
        }
    except Exception:
        return {}


def parse_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def parse_utc_datetime(iso_str):
    if not iso_str:
        return None
    s = str(iso_str).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def humanize_time_display(iso_str):
    dt = parse_utc_datetime(iso_str)
    if dt is None:
        return iso_str or "—"
    local = dt.astimezone(DISPLAY_TZ)
    wd = local.strftime("%a")
    day = local.day
    mon = local.strftime("%b")
    year = local.year
    h24 = local.hour
    h12 = h24 % 12 or 12
    ampm = "AM" if h24 < 12 else "PM"
    clock = f"{h12}:{local.strftime('%M:%S')} {ampm}"
    abs_part = f"{wd} {day} {mon} {year}, {clock} {DISPLAY_TZ_LABEL}"

    now = datetime.now(timezone.utc)
    secs = int((now - dt).total_seconds())
    rel = None
    if secs < 0:
        rel = "soon"
    elif secs < 45:
        rel = "just now"
    elif secs < 3600:
        m = max(1, secs // 60)
        rel = "1 minute ago" if m == 1 else f"{m} minutes ago"
    elif secs < 86400:
        h = max(1, secs // 3600)
        rel = "1 hour ago" if h == 1 else f"{h} hours ago"
    elif secs < 604800:
        d = max(1, secs // 86400)
        rel = "1 day ago" if d == 1 else f"{d} days ago"
    elif secs < 2592000:
        w = max(1, secs // 604800)
        rel = "1 week ago" if w == 1 else f"{w} weeks ago"

    if rel and rel != "soon":
        return f"{rel} · {abs_part}"
    if rel == "soon":
        return f"{abs_part} (in the future)"
    return abs_part


def visit_display_fields(row):
    d = dict(row)
    parts = []
    for key in ("ip_city", "ip_region", "ip_country"):
        if d.get(key):
            parts.append(str(d[key]))
    d["ip_area"] = ", ".join(parts) if parts else None
    glat, glon = d.get("gps_lat"), d.get("gps_lon")
    if glat is not None and glon is not None:
        acc = d.get("gps_accuracy_m")
        accs = f" ±{acc:.0f}m" if acc is not None else ""
        d["gps_cell"] = f"{glat:.5f}, {glon:.5f}{accs}"
    else:
        d["gps_cell"] = None
    d["visited_at_human"] = humanize_time_display(d.get("visited_at"))
    if glat is not None and glon is not None:
        d["visit_map_lat"] = float(glat)
        d["visit_map_lng"] = float(glon)
        d["visit_map_kind"] = "gps"
    elif d.get("ip_lat") is not None and d.get("ip_lon") is not None:
        d["visit_map_lat"] = float(d["ip_lat"])
        d["visit_map_lng"] = float(d["ip_lon"])
        d["visit_map_kind"] = "ip"
    else:
        d["visit_map_lat"] = None
        d["visit_map_lng"] = None
        d["visit_map_kind"] = None
    return d


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/create", methods=["POST"])
def api_create():
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "URL is required"}), 400
    if not allowed_url(url):
        return jsonify({"error": "Only http and https URLs with a host are allowed"}), 400

    slug = random_slug()
    stats_token = secrets.token_urlsafe(24)
    created = datetime.now(timezone.utc).isoformat()
    db = get_db()
    db.execute(
        "INSERT INTO links (slug, destination_url, stats_token, created_at) VALUES (?, ?, ?, ?)",
        (slug, url, stats_token, created),
    )
    db.commit()

    base = request.host_url.rstrip("/")
    return jsonify(
        {
            "short_url": f"{base}/l/{slug}",
            "stats_url": f"{base}/stats/{slug}?token={stats_token}",
            "slug": slug,
        }
    )


def client_ip():
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.remote_addr or ""


@app.route("/l/<slug>")
def go(slug):
    db = get_db()
    row = db.execute(
        "SELECT id, destination_url FROM links WHERE slug = ?", (slug,)
    ).fetchone()
    if not row:
        return render_template("missing.html"), 404

    visited = datetime.now(timezone.utc).isoformat()
    ip = client_ip()
    cur = db.execute(
        "INSERT INTO visits (link_id, ip, user_agent, referrer, visited_at) VALUES (?, ?, ?, ?, ?)",
        (
            row["id"],
            ip,
            request.headers.get("User-Agent", "")[:2000],
            request.headers.get("Referer", "")[:2000],
            visited,
        ),
    )
    visit_id = cur.lastrowid
    # IP geolocation runs in finish_visit only if GPS is not saved (try GPS first on client).

    token = secrets.token_urlsafe(24)
    expires = (datetime.now(timezone.utc) + FINISH_TOKEN_TTL).isoformat()
    db.execute(
        "INSERT INTO visit_finish_tokens (token, visit_id, expires_at) VALUES (?, ?, ?)",
        (token, visit_id, expires),
    )
    db.commit()

    return render_template("continue.html", token=token)


def _finish_payload():
    if request.method == "GET":
        return {"token": request.args.get("token", "")}
    if request.content_type and "application/json" in request.content_type:
        return request.get_json(silent=True) or {}
    return request.form.to_dict()


@app.route("/finish-visit", methods=["GET", "POST"])
def finish_visit():
    payload = _finish_payload()
    token = (payload.get("token") or "").strip()
    if not token:
        if request.method != "GET" and (
            request.content_type and "application/json" in request.content_type
        ):
            return jsonify({"error": "token required"}), 400
        return "Bad request", 400

    db = get_db()
    tok = db.execute(
        "SELECT visit_id, expires_at FROM visit_finish_tokens WHERE token = ?", (token,)
    ).fetchone()
    if not tok:
        if request.method != "GET" and (
            request.content_type and "application/json" in request.content_type
        ):
            return jsonify({"error": "invalid token"}), 403
        return "Invalid or expired link", 403

    try:
        exp = datetime.fromisoformat(tok["expires_at"].replace("Z", "+00:00"))
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        exp = None
    if exp is None or datetime.now(timezone.utc) > exp:
        db.execute("DELETE FROM visit_finish_tokens WHERE token = ?", (token,))
        db.commit()
        if request.method != "GET" and (
            request.content_type and "application/json" in request.content_type
        ):
            return jsonify({"error": "expired"}), 403
        return "Invalid or expired link", 403

    visit_id = tok["visit_id"]

    pt = db.execute(
        """SELECT lat, lng, accuracy FROM visit_location_points
           WHERE visit_id = ? ORDER BY id DESC LIMIT 1""",
        (visit_id,),
    ).fetchone()

    gps_ok = False
    if pt:
        db.execute(
            "UPDATE visits SET gps_lat=?, gps_lon=?, gps_accuracy_m=? WHERE id=?",
            (pt["lat"], pt["lng"], pt["accuracy"], visit_id),
        )
        gps_ok = True
    else:
        lat = parse_float(payload.get("lat"))
        lng = parse_float(payload.get("lng"))
        acc = parse_float(payload.get("accuracy"))
        if lat is not None and lng is not None:
            if not (-90 <= lat <= 90 and -180 <= lng <= 180):
                lat, lng, acc = None, None, None
            else:
                db.execute(
                    "UPDATE visits SET gps_lat=?, gps_lon=?, gps_accuracy_m=? WHERE id=?",
                    (lat, lng, acc, visit_id),
                )
                gps_ok = True

    if not gps_ok:
        vrow = db.execute("SELECT ip FROM visits WHERE id = ?", (visit_id,)).fetchone()
        ip_val = (vrow["ip"] if vrow else "") or ""
        geo = lookup_ip_geo(ip_val)
        if geo:
            db.execute(
                """UPDATE visits SET ip_country=?, ip_region=?, ip_city=?, ip_lat=?, ip_lon=?
                   WHERE id=?""",
                (
                    geo.get("country"),
                    geo.get("region"),
                    geo.get("city"),
                    geo.get("lat"),
                    geo.get("lon"),
                    visit_id,
                ),
            )

    dest_row = db.execute(
        """
        SELECT l.destination_url FROM links l
        JOIN visits v ON v.link_id = l.id
        WHERE v.id = ?
        """,
        (visit_id,),
    ).fetchone()
    dest_url = dest_row["destination_url"] if dest_row else "/"

    db.execute("DELETE FROM visit_finish_tokens WHERE token = ?", (token,))
    db.commit()

    wants_json = request.method == "POST" and (
        request.content_type and "application/json" in request.content_type
    )
    if wants_json:
        return jsonify({"redirect": dest_url})
    return redirect(dest_url, code=302)


def _visit_id_for_track_token(token):
    token = (token or "").strip()
    if not token:
        return None
    db = get_db()
    tok = db.execute(
        "SELECT visit_id, expires_at FROM visit_finish_tokens WHERE token = ?", (token,)
    ).fetchone()
    if not tok:
        return None
    try:
        exp = datetime.fromisoformat(tok["expires_at"].replace("Z", "+00:00"))
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None
    if datetime.now(timezone.utc) > exp:
        return None
    return tok["visit_id"]


@app.route("/api/visit-track", methods=["POST"])
def visit_track():
    data = request.get_json(silent=True) or {}
    visit_id = _visit_id_for_track_token(data.get("token", ""))
    if visit_id is None:
        return jsonify({"error": "invalid token"}), 403

    lat = parse_float(data.get("lat"))
    lng = parse_float(data.get("lng"))
    acc = parse_float(data.get("accuracy"))
    if lat is None or lng is None or not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return jsonify({"error": "bad coordinates"}), 400

    db = get_db()
    n = db.execute(
        "SELECT COUNT(*) AS c FROM visit_location_points WHERE visit_id = ?",
        (visit_id,),
    ).fetchone()["c"]
    if n >= 120:
        return jsonify({"ok": True, "capped": True})

    rec = datetime.now(timezone.utc).isoformat()
    db.execute(
        """INSERT INTO visit_location_points (visit_id, lat, lng, accuracy, recorded_at)
           VALUES (?, ?, ?, ?, ?)""",
        (visit_id, lat, lng, acc, rec),
    )
    db.commit()
    return jsonify({"ok": True})


def _build_visit_tracks_json(db, visit_ids):
    if not visit_ids:
        return {}
    placeholders = ",".join("?" * len(visit_ids))
    q = (
        "SELECT visit_id, lat, lng, accuracy, recorded_at FROM visit_location_points "
        f"WHERE visit_id IN ({placeholders}) ORDER BY visit_id, id ASC"
    )
    rows = db.execute(q, visit_ids).fetchall()
    by_vid = {}
    for r in rows:
        vid = r["visit_id"]
        by_vid.setdefault(vid, []).append(
            {
                "lat": float(r["lat"]),
                "lng": float(r["lng"]),
                "acc": r["accuracy"],
                "t": r["recorded_at"],
            }
        )
    return {str(k): v for k, v in by_vid.items()}


@app.route("/stats/<slug>")
def stats(slug):
    token = request.args.get("token", "")
    db = get_db()
    row = db.execute(
        "SELECT id, destination_url, stats_token, created_at FROM links WHERE slug = ?",
        (slug,),
    ).fetchone()
    if not row or not secrets.compare_digest(row["stats_token"], token):
        return render_template("stats_denied.html"), 403

    raw = db.execute(
        """
        SELECT id, ip, user_agent, referrer, visited_at,
               ip_country, ip_region, ip_city, ip_lat, ip_lon,
               gps_lat, gps_lon, gps_accuracy_m
        FROM visits WHERE link_id = ?
        ORDER BY id DESC
        LIMIT 500
        """,
        (row["id"],),
    ).fetchall()

    visits = [visit_display_fields(r) for r in raw]
    visit_ids = [v["id"] for v in visits if v.get("id") is not None]
    visit_tracks = _build_visit_tracks_json(db, visit_ids)
    has_visit_maps = any(v.get("visit_map_lat") is not None for v in visits)
    needs_leaflet = has_visit_maps or bool(visit_tracks)

    return render_template(
        "stats.html",
        slug=slug,
        destination=row["destination_url"],
        created_at=row["created_at"],
        created_at_human=humanize_time_display(row["created_at"]),
        visits=visits,
        visit_count=len(visits),
        has_visit_maps=has_visit_maps,
        visit_tracks=visit_tracks,
        needs_leaflet=needs_leaflet,
    )


init_db()


if __name__ == "__main__":
    # Default 5001: macOS often binds 5000 to AirPlay Receiver (Sharing).
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5001)), debug=True)
