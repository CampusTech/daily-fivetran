# daily-fivetran

A [Fivetran Connector SDK](https://fivetran.com/docs/connector-sdk) connector that syncs [Daily.co](https://www.daily.co) usage metadata into your warehouse. You can use it for recording, transcription and video cost analysis in SQL.

It syncs **metadata only**: no recording media, transcript text, download links, storage paths or participant names (see [Data handling](#data-handling)).

## Tables

| Table | Source | Primary key | Sync |
|---|---|---|---|
| `rooms` | `GET /rooms` | `id` | Full refresh, at most once per 24h |
| `recordings` | `GET /recordings` | `id` | Incremental: newest-first walk, stops at `start_ts < cursor - 3 days` |
| `transcripts` | `GET /transcript` | `transcript_id` | Incremental: newest-first walk, stops at `created_at < cursor - 3 days` |
| `meetings` | `GET /meetings` | `id` | Incremental: newest-first walk, stops at `start_time < cursor - 2 h`, or earlier to cover meetings still ongoing last run |
| `meeting_participants` | `participants` inline in `GET /meetings` | `meeting_id`, `participant_id` | With `meetings` |
| `batch_processor_jobs` | `GET /batch-processor` | `id` | Incremental: newest-first walk until 5 pages past the newest job seen last run. Jobs have no timestamps, and the re-read pages catch status changes. |

Every row has a `domain` column. Field names are snake_cased (`mtgSessionId` becomes `mtg_session_id`). Unlisted API fields flow through, and Fivetran infers their types.

The lookback re-reads in-progress recordings, transcripts and meetings, whose `status` and `duration` change after they're first seen. Upserts make reruns idempotent.

## Configuration

`configuration.json` (gitignored; see `configuration.example.json`):

| Key | Required | Description |
|---|---|---|
| `api_key` | yes | Daily REST API key |
| `domain` | yes | Daily subdomain the key belongs to (`acme` for `acme.daily.co`). The sync fails if the key belongs to a different domain. |
| `start_date` | no | First-sync backfill start, `YYYY-MM-DD`. Default `2025-01-01`. |

Daily API keys are per-domain. To sync several domains, deploy one connection per domain, each with its own `api_key` and `domain`.

## Run locally

```bash
python3 -m venv venv && source venv/bin/activate
pip install fivetran-connector-sdk requests
cp configuration.example.json configuration.json   # fill in
python test_connector.py                           # offline checks
python connector.py                                # full sync into files/warehouse.db (DuckDB)
```

## Deploy

`.github/workflows/deploy.yml` runs the offline tests, then `fivetran deploy` on every push to `main`. It needs these settings:

- Secrets: `DAILY_API_KEY`, `DAILY_DOMAIN`, `FIVETRAN_API_KEY` (base64), `FIVETRAN_DESTINATION`, `FIVETRAN_CONNECTION` (the destination schema name).
- Optional variable: `DAILY_START_DATE`.

## Rate limits and retries

Daily allows about 50 requests per 30 s on data endpoints (recordings, transcripts, meetings, batch processor) and about 100 per 5 s on `/rooms`. The connector paces requests at 0.65 s and 0.1 s respectively.

- Timeouts and 5xx responses retry with exponential backoff.
- A 429 honours `Retry-After`, whether it's given in seconds or as an HTTP-date.

`/meetings` is slow on large domains: about 50 s per 100-row page, and any `timeframe_start`/`timeframe_end` filter can time out. So meetings are walked newest-first with no filter, and the request timeout is 180 s.

- Meetings only change while `ongoing`. Each run re-reads the last 2 h, or back to the oldest meeting that was still ongoing last run, capped at 3 days.
- Meetings sync last, so the other tables land first each run.
- The first backfill is slow: about 5,000 pages, or ~70 h, for a domain with 500k meetings. It saves progress every page and resumes on the next sync. Set `start_date` to bound it.

## Data handling

These keys are stripped recursively from every payload before upsert (see `sanitize()` in `connector.py`):

- Storage paths: `s3key`, `outParams`, `bucket`, `recordings_bucket`, `transcription_bucket`, `s3Config`, `recordings_template`, `transcription_template`, batch-job `output`.
- Access: `share_token`, `url`, room `meeting_join_hook`, `streaming_endpoints`, SIP/dial-in config, access/download links, and any key containing `token`, `secret`, `password`, `apikey`, `hmac` or `credential`.
- Personal data: `user_name`. `user_id` is kept so you can join to your own users.
- Room `config.enable_transcription` can hold `deepgram:<api key>`. Only the provider name is kept.

Room `config` is **allowlisted** (`ROOM_CONFIG_PREFIXES` / `ROOM_CONFIG_KEYS`): only feature flags (`enable_*`, `start_*`), limits (`max_*`, `eject_*`), `auto_transcription_settings`, `permissions` and a few scalar settings are kept. New config keys Daily adds are dropped until someone adds them to the allowlist.

The connector never calls:

- domain config (`GET /`), which can hold secrets;
- `/recordings/:id/access-link`, `/transcript/:id/access-link` or batch-job access links;
- presence, logs or session data.

## License

MIT
