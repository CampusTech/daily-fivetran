"""Offline checks: python test_connector.py"""

from unittest import mock


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
        "max_participants": 10, "meeting_join_hook": "https://h?k=1",
        "streaming_endpoints": [{"name": "yt", "endpoint": "rtmp://x/key"}],
        "sip": {"display_name": "Bob", "sip_mode": "dial-in"}, "recordings_template": "{room}/x",
        "some_future_integration": {"uri": "x"}}}
    assert c.to_row(room, "d") == {"domain": "d", "name": "Labc", "config": {"enable_transcription": "deepgram", "max_participants": 10}}
    assert c.to_row({"user_id": "u", "user_name": "Alice", "participant_id": "p"}, "d") == {"domain": "d", "user_id": "u", "participant_id": "p"}


def test_retry_after():
    assert c.retry_after_seconds("3", 9) == 3
    assert c.retry_after_seconds(None, 9) == 9
    assert c.retry_after_seconds("Wed, 21 Oct 2015 07:28:00 GMT", 9) == 0
    assert c.retry_after_seconds("garbage", 9) == 9


class FakeClient:
    """Serves newest-first rows 100 per page."""

    def __init__(self, rows=None):
        self.rows = rows or []
        self.calls = []
        self.domain = "d"

    def pages(self, path, params=None, id_key="id", max_tries=6):
        params = params or {}
        self.calls.append((path, dict(params)))
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


def test_meetings_floor_follows_ongoing():
    now = 1_800_000_000
    # newest first, every 10 min; the one 5 h old is still ongoing
    rows = [{"id": f"m{i}", "start_time": now - i * 600, "ongoing": i == 30,
             "participants": [{"participant_id": f"p{i}", "user_id": "u", "user_name": "X"}]}
            for i in range(200)]
    state = {"meetings_cursor": now - 600}
    out = run(c.sync_newest_first, FakeClient(rows), state, "/meetings", "meetings", "id", "start_time", 0,
              c.MEETINGS_LOOKBACK_SECONDS, c.write_meeting)
    meetings = [d for t, d in out if t == "meetings"]
    parts = [d for t, d in out if t == "meeting_participants"]
    assert len(meetings) == 14  # floor = cursor - 2 h -> rows 0..13
    assert parts[0] == {"meeting_id": "m0", "domain": "d", "participant_id": "p0", "user_id": "u"}
    assert state["meetings_open_min"] is None

    # last run saw m30 ongoing -> floor extends back to it
    rows = [dict(r, participants=[]) for r in rows]
    state = {"meetings_cursor": now - 600, "meetings_open_min": now - 30 * 600}
    out = run(c.sync_newest_first, FakeClient(rows), state, "/meetings", "meetings", "id", "start_time", 0,
              c.MEETINGS_LOOKBACK_SECONDS, c.write_meeting)
    assert len(out) == 31
    assert state["meetings_open_min"] == now - 30 * 600


def test_batch_jobs_stop_after_known():
    rows = [{"id": f"j{i}", "preset": "p", "status": "finished"} for i in range(1000)]
    state = {}
    out = run(c.sync_batch_jobs, FakeClient(rows), state, 2)
    assert len(out) == 1000 and state["batch_processor_jobs_newest"] == "j0"

    rows = [{"id": f"n{i}", "status": "submitted"} for i in range(150)] + rows  # 150 new jobs
    out = run(c.sync_batch_jobs, FakeClient(rows), state, 2)
    # known j0 is on page 2 -> read page 2 and one more
    assert len(out) == 300, len(out)
    assert state["batch_processor_jobs_newest"] == "n0" and "batch_processor_jobs_resume" not in state


def test_meetings_incremental_params_only_after_backfill():
    rows = [{"id": f"m{i}", "start_time": 1_800_000_000 - i * 600, "participants": []} for i in range(50)]
    params = lambda floor: {"timeframe_start": int(floor), "lookback_days": "15"}
    client = FakeClient(rows)
    run(c.sync_newest_first, client, {}, "/meetings", "meetings", "id", "start_time", 0,
        c.MEETINGS_LOOKBACK_SECONDS, c.write_meeting, params)
    assert client.calls[0][1] == {}  # backfill: unfiltered

    client = FakeClient(rows)
    state = {"meetings_cursor": 1_800_000_000}
    run(c.sync_newest_first, client, state, "/meetings", "meetings", "id", "start_time", 0,
        c.MEETINGS_LOOKBACK_SECONDS, c.write_meeting, params)
    assert client.calls[0][1] == {"timeframe_start": 1_800_000_000 - 7200, "lookback_days": "15"}


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
