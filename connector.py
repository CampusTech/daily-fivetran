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

# /meetings takes ~50 s per page on large domains and times out on any
# timeframe filter, so it is walked newest-first like recordings. Meetings only
# change while ongoing: re-read 2 h, or back to the oldest meeting that was
# still ongoing last run (capped at LOOKBACK_SECONDS).
MEETINGS_LOOKBACK_SECONDS = 2 * 3600

# Keys dropped anywhere in a payload, compared lowercased with "_" removed.
# s3key/outparams/bucket/output: storage paths. share_token/url: access.
# user_name: participant display names (FERPA); user_id joins to the replica.
DROP_KEYS = {
    "s3key", "sharetoken", "outparams", "username", "url", "output",
    "recordingsbucket", "transcriptionbucket", "s3config", "bucket",
    "accesslink", "downloadlink", "assumerolearn",
    "recordingstemplate", "transcriptiontemplate",
    # room config: webhook URL, RTMP endpoints (stream keys), SIP/dial-in
    "meetingjoinhook", "streamingendpoints", "endpoint", "extraendpoints",
    "sip", "sipuri", "dialinconfig", "dialoutconfig", "displayname",
}
DROP_KEY_PARTS = ("secret", "token", "password", "apikey", "hmac", "credential")


# Room config is allowlisted: Daily keeps adding integration settings (hooks,
# RTMP, SIP, buckets) that can carry URLs or credentials. Feature flags and
# limits pass; anything new stays out until added here.
ROOM_CONFIG_PREFIXES = ("enable_", "start_", "max_", "eject_")
ROOM_CONFIG_KEYS = {
    "auto_transcription_settings", "permissions", "exp", "nbf", "lang", "geo",
    "owner_only_broadcast", "experimental_optimize_large_calls", "sfu_switchover",
    "enforce_unique_user_ids", "allow_room_start", "storage_provider",
}


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
    if isinstance(obj.get("config"), dict):
        obj = {**obj, "config": {k: v for k, v in obj["config"].items()
                                 if k.startswith(ROOM_CONFIG_PREFIXES) or k in ROOM_CONFIG_KEYS}}
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
                resp = self.session.get(f"{DAILY_API_BASE}{path}", params=params, timeout=180)
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


def write_row(table: str, obj: dict, domain: str):
    op.upsert(table, to_row(obj, domain))


def write_meeting(table: str, obj: dict, domain: str):
    participants = obj.pop("participants", None) or []
    op.upsert(table, to_row(obj, domain))
    for p in participants:
        op.upsert("meeting_participants", {"meeting_id": obj["id"], **to_row(p, domain)})


def sync_newest_first(client: DailyClient, state: dict, path: str, table: str,
                      id_key: str, ts_key: str, start_epoch: float,
                      lookback: float = LOOKBACK_SECONDS, write=write_row):
    """Walk a newest-first list until rows are older than the floor.

    No server-side "updated since" filter exists, so this is the incremental
    strategy. Floor = cursor - lookback, extended back to the oldest row that
    was `ongoing` last run (capped at LOOKBACK_SECONDS). A partial walk is
    resumable via state[f"{table}_resume"].
    """
    cursor = state.get(f"{table}_cursor")
    floor = start_epoch
    if cursor:
        floor = cursor - lookback
        if state.get(f"{table}_open_min"):
            floor = max(min(floor, state[f"{table}_open_min"]), cursor - LOOKBACK_SECONDS)
    resume = state.get(f"{table}_resume") or {}
    max_ts = resume.get("max_ts", cursor or 0)
    open_min = resume.get("open_min")
    params = {"starting_after": resume["starting_after"]} if resume.get("starting_after") else {}

    n = 0
    done = False
    for page in client.pages(path, params, id_key=id_key):
        for obj in page:
            ts = to_epoch(obj.get(ts_key))
            if ts is not None and ts < floor:
                done = True
                break
            if obj.get("ongoing") and ts is not None:
                open_min = min(open_min or ts, ts)
            write(table, obj, client.domain)
            n += 1
            if ts is not None:
                max_ts = max(max_ts, ts)
        if done:
            break
        state[f"{table}_resume"] = {"starting_after": page[-1][id_key], "max_ts": max_ts, "open_min": open_min}
        op.checkpoint(state)

    # ponytail: if the resume row was deleted upstream the walk may end early and
    # skip older rows; a full resync (clear state) recovers.
    state.pop(f"{table}_resume", None)
    state[f"{table}_open_min"] = open_min
    if max_ts:
        state[f"{table}_cursor"] = max_ts
    op.checkpoint(state)
    log.info(f"{table}: {n} rows, cursor={max_ts}")


def sync_batch_jobs(client: DailyClient, state: dict, recheck_pages: int = 5):
    """Batch jobs have no timestamps: walk newest-first until `recheck_pages`
    pages past the newest job seen last run (re-reading those catches status
    changes on recent jobs). Resumable like sync_newest_first."""
    table = "batch_processor_jobs"
    last_newest = state.get(f"{table}_newest")
    resume = state.get(f"{table}_resume") or {}
    newest, left = resume.get("newest"), resume.get("left")
    params = {"starting_after": resume["starting_after"]} if resume.get("starting_after") else {}

    n = 0
    for page in client.pages("/batch-processor", params):
        newest = newest or page[0]["id"]
        for obj in page:
            write_row(table, obj, client.domain)
        n += len(page)
        if left is None and any(o["id"] == last_newest for o in page):
            left = recheck_pages
        if left is not None:
            left -= 1
            if left <= 0:
                break
        state[f"{table}_resume"] = {"starting_after": page[-1]["id"], "newest": newest, "left": left}
        op.checkpoint(state)

    state.pop(f"{table}_resume", None)
    if newest:
        state[f"{table}_newest"] = newest
    op.checkpoint(state)
    log.info(f"{table}: {n} rows")


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
    sync_batch_jobs(client, state)
    # Last: the initial backfill takes days; checkpoints resume it next sync.
    sync_newest_first(client, state, "/meetings", "meetings", "id", "start_time", start_epoch,
                      lookback=MEETINGS_LOOKBACK_SECONDS, write=write_meeting)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    import json

    with open("configuration.json") as f:
        connector.debug(configuration=json.load(f))
