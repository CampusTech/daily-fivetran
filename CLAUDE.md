# daily-fivetran

Fivetran Connector SDK connector for the Daily.co REST API. README.md covers tables, sync strategy, config and data handling.

## Files

- `connector.py`: the whole connector (`schema()`, `update()`, `DailyClient`, `sanitize()`).
- `test_connector.py`: offline assert-based checks (`python test_connector.py`). They run in CI before deploy.
- `requirements.txt`: intentionally empty. Fivetran's runtime provides `requests` and the SDK.
- `configuration.json`: local only, gitignored. Never commit it.

## Rules

- **Metadata only.** Never add calls to access-link endpoints, domain config (`GET /v1/`), presence, logs or session data. Never sync transcript text or media.
- New fields must pass through `sanitize()`. If a new endpoint returns storage paths, links, names or secrets, add the key to `DROP_KEYS` and a case to `test_sanitize`.
- Room `config` is allowlisted. Only add a key to `ROOM_CONFIG_KEYS` after confirming it can't hold URLs, paths or credentials.
- Data endpoints are rate limited to about 50 requests per 30 s. Don't lower `DEFAULT_PACE_SECONDS` without checking Daily's current rate-limit docs.
- Changing a primary key requires a full resync of the destination table.
- State keys:
  - `{table}_cursor`: epoch seconds.
  - `{table}_resume`: a mid-walk resume point.
  - `meetings_cursor`: end of the last finished window.
  - `rooms_synced_at`: time of the last rooms refresh.

  Clearing state triggers a full backfill from `start_date`.
