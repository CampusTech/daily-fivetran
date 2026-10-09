"""Offline checks: python test_connector.py"""

from unittest import mock

import requests

import connector as c

c.log.LOG_LEVEL = c.log.Level.WARNING


def test_sanitize():
    rec = {
        "id": "r1", "s3key": "bucket/path", "share_token": "abc", "mtgSessionId": "m1",
        "tracks": [{"type": "audio", "s3Key": "x", "size": 3}],
    }
    assert c.to_row(rec, "d") == {"domain": "d", "id": "r1", "mtg_session_id": "m1", "tracks": [{"type": "audio", "size": 3}]}
    t = {"transcriptId": "t1", "outParams": {"bucket": "b"}, "isVttAvailable": True, "participantMinutes": 4.5}
    assert c.to_row(t, "d") == {"domain": "d", "transcript_id": "t1", "is_vtt_available": True, "participant_minutes": 4.5}
    room = {"name": "Labc", "url": "https://x", "config": {
        "enable_transcription": "deepgram:SECRETKEY", "recordings_bucket": {"bucket_name": "b"},
        "max_participants": 10}}
    assert c.to_row(room, "d") == {"domain": "d", "name": "Labc", "config": {"enable_transcription": "deepgram", "max_participants": 10}}
    assert c.to_row({"user_id": "u", "user_name": "Alice", "participant_id": "p"}, "d") == {"domain": "d", "user_id": "u", "participant_id": "p"}


def test_retry_after():
    assert c.retry_after_seconds("3", 9) == 3
    assert c.retry_after_seconds(None, 9) == 9
    assert c.retry_after_seconds("Wed, 21 Oct 2015 07:28:00 GMT", 9) == 0
    assert c.retry_after_seconds("garbage", 9) == 9


class FakeClient:
    """Serves newest-first rows 100 per page; can 504 /meetings for wide windows."""

    def __init__(self, rows=None, meetings_max_window=None):
        self.rows = rows or []
        self.meetings_max_window = meetings_max_window
        self.calls = []
        self.domain = "d"

    def pages(self, path, params=None, id_key="id", max_tries=6):
        params = params or {}
        self.calls.append((path, dict(params)))
        if path == "/meetings":
            if self.meetings_max_window and params["timeframe_end"] - params["timeframe_start"] > self.meetings_max_window:
                resp = requests.Response()
                resp.status_code = 504
                raise requests.HTTPError(response=resp)
            return iter([])
        rows = self.rows
        if params.get("starting_after"):
            i = [r[id_key] for r in rows].index(params["starting_after"])
            rows = rows[i + 1:]
        return iter([rows[i:i + 100] for i in range(0, len(rows), 100)])


def run(fn, *args):
    out = []
    with mock.patch.object(c.op, "upsert", lambda t, d: out.append((t, d))), \
            mock.patch.object(c.op, "checkpoint", lambda s: None):
        fn(*args)
    return out


def test_newest_first_stops_at_lookback():
    now = 1_800_000_000
    rows = [{"id": f"r{i}", "start_ts": now - i * 3600} for i in range(500)]  # hourly, newest first
    state = {"recordings_cursor": now - 24 * 3600}
    out = run(c.sync_newest_first, FakeClient(rows), state, "/recordings", "recordings", "id", "start_ts", 0)
    # floor = cursor - 3 days = now - 96h, so rows 0..96 inclusive
    assert len(out) == 97, len(out)
    assert state["recordings_cursor"] == now
    assert "recordings_resume" not in state

    # first run: floor is start_date
    state = {}
    out = run(c.sync_newest_first, FakeClient(rows), state, "/recordings", "recordings", "id", "start_ts", now - 10 * 3600)
    assert len(out) == 11

    # resume continues after the saved id with the saved max_ts
    state = {"recordings_resume": {"starting_after": "r99", "max_ts": now}}
    out = run(c.sync_newest_first, FakeClient(rows), state, "/recordings", "recordings", "id", "start_ts", now - 200 * 3600)
    assert out[0][1]["id"] == "r100" and len(out) == 101
    assert state["recordings_cursor"] == now


def test_meetings_window_shrinks_on_504():
    start = 1_800_000_000
    client = FakeClient(meetings_max_window=600)
    state = {}
    with mock.patch.object(c.time, "time", lambda: start + 7200):
        run(c.sync_meetings, client, state, start)
    widths = [p["timeframe_end"] - p["timeframe_start"] for _, p in client.calls]
    assert widths[:4] == [3600, 1800, 900, 450], widths
    assert state["meetings_cursor"] == start + 7200


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
