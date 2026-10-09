"""
Fivetran Connector for Daily.co

Syncs usage metadata only (no media, transcript text, links or secrets):
- rooms: room list + config (full refresh, at most once per 24h)
- recordings: incremental, newest-first walk with lookback
- transcripts: incremental, newest-first walk with lookback
- meetings / meeting_participants: incremental, forward time windows that
  shrink on 504s
- batch_processor_jobs: job metadata (full refresh)

See CLAUDE.md for the data-handling rules this file enforces.
"""

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import re
from urllib.parse import urlparse
import time

import requests
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

DAILY_API_BASE = "https://api.daily.co/v1"
PAGE_SIZE = 100
LOOKBACK_SECONDS = 3 * 86400  # in-progress recordings/transcripts/meetings mutate
ROOMS_REFRESH_SECONDS = 86400
DEFAULT_START_DATE = "2025-01-01"

# Daily rate limits (docs.daily.co/docs/rest-api#rate-limits): data endpoints
# (recordings, transcripts, meetings, batch processor) ~50 req / 30 s; rooms
# ~100 req / 5 s. Pace below both; 429s are still handled.
PACE_SECONDS = {"/rooms": 0.1}
DEFAULT_PACE_SECONDS = 0.65

MEETING_WINDOW_MIN = 300
MEETING_WINDOW_START = 3600
MEETING_WINDOW_MAX = 86400

# Keys dropped anywhere in a payload, compared lowercased with "_" removed.
# s3key/outparams/bucket/output: storage paths. share_token/url: access.
# user_name: participant display names (FERPA); user_id joins to the replica.
DROP_KEYS = {
    "s3key", "sharetoken", "outparams", "username", "url", "output",
    "recordingsbucket", "transcriptionbucket", "s3config", "bucket",
    "accesslink", "downloadlink",
}
DROP_KEY_PARTS = ("secret", "token", "password", "apikey", "hmac", "credential")


def _norm(key: str) -> str:
    return key.lower().replace("_", "")


def sanitize(value):
    """Recursively strip storage paths, access tokens, secrets and names."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            n = _norm(k)
            if n in DROP_KEYS or any(p in n for p in DROP_KEY_PARTS):
                continue
            # Daily accepts "deepgram:<api key>" here; keep only the provider.
            if n == "enabletranscription" and isinstance(v, str):
                v = v.split(":", 1)[0]
            out[k] = sanitize(v)
        return out
    if isinstance(value, list):
        return [sanitize(v) for v in value]
    return value


def snake(key: str) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key).lower()


def to_row(obj: dict, domain: str) -> dict:
    return {"domain": domain, **{snake(k): v for k, v in sanitize(obj).items()}}


def to_epoch(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def retry_after_seconds(header: str | None, default: float) -> float:
    """Retry-After is either delta-seconds or an HTTP-date."""
    if not header:
        return default
    try:
        return max(0.0, float(header))
    except ValueError:
        pass
    try:
        return max(0.0, (parsedate_to_datetime(header) - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError):
        return default


class DailyClient:
    def __init__(self, api_key: str, domain: str):
        self.domain = domain
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {api_key}"
        self.last_call = 0.0

    def get(self, path: str, params: dict | None = None, max_tries: int = 6) -> dict:
        pace = PACE_SECONDS.get(path, DEFAULT_PACE_SECONDS)
        for attempt in range(max_tries):
            wait = self.last_call + pace - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self.last_call = time.monotonic()
            backoff = min(2 ** attempt, 60)
            try:
                resp = self.session.get(f"{DAILY_API_BASE}{path}", params=params, timeout=60)
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt == max_tries - 1:
                    raise
                log.warning(f"GET {path} failed ({e}); retry in {backoff}s")
                time.sleep(backoff)
                continue
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt == max_tries - 1:
                    resp.raise_for_status()
                if resp.status_code == 429:
                    backoff = retry_after_seconds(resp.headers.get("Retry-After"), backoff)
                log.warning(f"GET {path} -> {resp.status_code}; retry in {backoff:.1f}s")
                time.sleep(backoff)
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError("unreachable")

    def pages(self, path: str, params: dict | None = None, id_key: str = "id", max_tries: int = 6):
        """Yield pages of `data` using starting_after cursor pagination."""
        params = {**(params or {}), "limit": PAGE_SIZE}
        while True:
            data = self.get(path, params, max_tries=max_tries).get("data") or []
            if data:
                yield data
            if len(data) < PAGE_SIZE:
                return
            params["starting_after"] = data[-1][id_key]


def check_domain(client: DailyClient):
    """API keys are per-domain; refuse to sync if the key isn't for `domain`."""
    rooms = client.get("/rooms", {"limit": 1}).get("data") or []
    if rooms:
        host = urlparse(rooms[0].get("url", "")).hostname or ""
        if host.split(".")[0] != client.domain:
            raise ValueError(f"api_key belongs to domain {host!r}, not {client.domain!r}")


def schema(configuration: dict):
    return [
        {
            "table": "rooms",
            "primary_key": ["id"],
            "columns": {
                "domain": "STRING",
                "id": "STRING",
                "name": "STRING",
                "api_created": "BOOLEAN",
                "privacy": "STRING",
                "created_at": "UTC_DATETIME",
                "config": "JSON",
            },
        },
        {
            "table": "recordings",
            "primary_key": ["id"],
            "columns": {
                "domain": "STRING",
                "id": "STRING",
                "room_name": "STRING",
                "start_ts": "LONG",
                "duration": "LONG",
                "status": "STRING",
                "max_participants": "INT",
                "mtg_session_id": "STRING",
                "tracks": "JSON",
            },
        },
        {
            "table": "transcripts",
            "primary_key": ["transcript_id"],
            "columns": {
                "domain": "STRING",
                "transcript_id": "STRING",
                "room_name": "STRING",
                "room_id": "STRING",
                "mtg_session_id": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "duration": "LONG",
                "participant_minutes": "DOUBLE",
                "status": "STRING",
                "is_vtt_available": "BOOLEAN",
            },
        },
        {
            "table": "meetings",
            "primary_key": ["id"],
            "columns": {
                "domain": "STRING",
                "id": "STRING",
                "room": "STRING",
                "start_time": "LONG",
                "duration": "LONG",
                "ongoing": "BOOLEAN",
                "max_participants": "INT",
            },
        },
        {
            "table": "meeting_participants",
            "primary_key": ["meeting_id", "participant_id"],
            "columns": {
                "domain": "STRING",
                "meeting_id": "STRING",
                "participant_id": "STRING",
                "user_id": "STRING",
                "join_time": "LONG",
                "duration": "LONG",
            },
        },
        {
            "table": "batch_processor_jobs",
            "primary_key": ["id"],
            "columns": {
                "domain": "STRING",
                "id": "STRING",
                "preset": "STRING",
                "status": "STRING",
                "input": "JSON",
            },
        },
    ]


def sync_full(client: DailyClient, path: str, table: str):
    n = 0
    for page in client.pages(path):
        for obj in page:
            op.upsert(table, to_row(obj, client.domain))
        n += len(page)
    log.info(f"{table}: {n} rows")


def sync_newest_first(client: DailyClient, state: dict, path: str, table: str,
                      id_key: str, ts_key: str, start_epoch: float):
    """Walk a newest-first list until rows are older than cursor - lookback.

    No server-side "updated since" filter exists, so this is the incremental
    strategy. A partial walk is resumable via state[f"{table}_resume"].
    """
    cursor = state.get(f"{table}_cursor")
    floor = cursor - LOOKBACK_SECONDS if cursor else start_epoch
    resume = state.get(f"{table}_resume") or {}
    max_ts = resume.get("max_ts", cursor or 0)
    params = {"starting_after": resume["starting_after"]} if resume.get("starting_after") else {}

    n = 0
    done = False
    for page in client.pages(path, params, id_key=id_key):
        for obj in page:
            ts = to_epoch(obj.get(ts_key))
            if ts is not None and ts < floor:
                done = True
                break
            op.upsert(table, to_row(obj, client.domain))
            n += 1
            if ts is not None:
                max_ts = max(max_ts, ts)
        if done:
            break
        state[f"{table}_resume"] = {"starting_after": page[-1][id_key], "max_ts": max_ts}
        op.checkpoint(state)

    # ponytail: if the resume row was deleted upstream the walk may end early and
    # skip older rows; a full resync (clear state) recovers.
    state.pop(f"{table}_resume", None)
    if max_ts:
        state[f"{table}_cursor"] = max_ts
    op.checkpoint(state)
    log.info(f"{table}: {n} rows, cursor={max_ts}")


def sync_meetings(client: DailyClient, state: dict, start_epoch: float):
    """Forward time windows over /meetings; shrink the window on 504s/timeouts."""
    cursor = state.get("meetings_cursor")
    window_start = int(cursor - LOOKBACK_SECONDS if cursor else start_epoch)
    window = MEETING_WINDOW_START
    ceiling, clean = MEETING_WINDOW_MAX, 0  # don't regrow into a window that just 504'd
    now = int(time.time())
    n = 0

    while window_start < now:
        window_end = min(window_start + window, now)
        params = {"timeframe_start": window_start, "timeframe_end": window_end}
        try:
            meetings = [m for page in client.pages("/meetings", params, max_tries=3) for m in page]
        except (requests.HTTPError, requests.Timeout, requests.ConnectionError) as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if (status is not None and status < 500) or window <= MEETING_WINDOW_MIN:
                raise
            ceiling, clean = window, 0
            window = max(window // 2, MEETING_WINDOW_MIN)
            log.warning(f"/meetings {window_start}-{window_end} failed ({e}); window -> {window}s")
            continue

        for m in meetings:
            participants = m.pop("participants", None) or []
            op.upsert("meetings", to_row(m, client.domain))
            for p in participants:
                op.upsert("meeting_participants", {"meeting_id": m["id"], **to_row(p, client.domain)})
        n += len(meetings)

        window_start = window_end
        state["meetings_cursor"] = window_end
        op.checkpoint(state)
        clean += 1
        if clean >= 24:
            ceiling = MEETING_WINDOW_MAX
        if len(meetings) < PAGE_SIZE and window * 2 < ceiling:
            window *= 2

    log.info(f"meetings: {n} rows, cursor={state.get('meetings_cursor')}")


def update(configuration: dict, state: dict):
    api_key = configuration.get("api_key")
    domain = configuration.get("domain")
    if not api_key or not domain:
        raise ValueError("Missing required configuration: api_key and domain")
    start_date = configuration.get("start_date") or DEFAULT_START_DATE
    start_epoch = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc).timestamp()

    client = DailyClient(api_key, domain)
    check_domain(client)
    state = state or {}

    if time.time() - state.get("rooms_synced_at", 0) >= ROOMS_REFRESH_SECONDS:
        sync_full(client, "/rooms", "rooms")
        state["rooms_synced_at"] = time.time()
        op.checkpoint(state)

    sync_newest_first(client, state, "/recordings", "recordings", "id", "start_ts", start_epoch)
    sync_newest_first(client, state, "/transcript", "transcripts", "transcriptId", "created_at", start_epoch)
    sync_meetings(client, state, start_epoch)

    sync_full(client, "/batch-processor", "batch_processor_jobs")
    op.checkpoint(state)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    import json

    with open("configuration.json") as f:
        connector.debug(configuration=json.load(f))
