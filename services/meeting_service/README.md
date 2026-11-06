# meeting-service

The LiveKit transport adapter: room-token mint, LiveKit webhooks, the server-side audio tap
to `transcription-service`, and recording egress into `mytheus-recordings`. Everything that
touches the LiveKit SDK lives in `transport.py`, behind `MeetingTransport`
(`tests/test_livekit_import_boundary.py` enforces that).

| Module | Role |
| --- | --- |
| `routes.py` | `POST /api/meetings/{id}/token` (consent gate, ADR-0018) |
| `webhook.py` | `POST /livekit/webhook`: participant events, `egress_ended`, `room_started` / `room_finished` |
| `consumer.py` | `meeting-events` consumer (own process: `python -m services.meeting_service.consumer`) |
| `transport.py` | `MeetingTransport`, `LiveKitTransport` (incl. `LiveKitAudioTap`, egress), `FakeTransport` |
| `audio_tap.py` | Provider-neutral tap plumbing: reconnecting WebSocket sink, backoff, tap registry |
| `audio_tap_worker.py` | Dedicated tap worker (own process: `python -m services.meeting_service.audio_tap_worker`), `AUDIO_TAP_MODE` |
| `room_lifecycle.py` | What `room_started` / `room_finished` do: consent + recording gates, tap, egress |
| `storage.py` | `mytheus-recordings` key convention, `RecordingConfig` |
| `bucket_lifecycle.py` | Bucket lifecycle rules + CLI (cold tier at 30 d, per-tenant expiry) |

## Room lifecycle

`room_started` (verified webhook, deduped on the room SID) then, after the transaction commits:

1. **Audio tap.** Joins as a hidden, subscribe-only participant `nexus-audio-tap-{meeting_id}`,
   subscribes to every remote audio track, has the SDK resample to 16 kHz mono int16, and sends
   each track over its own WebSocket to `TRANSCRIPTION_AUDIO_URL`. Rejoins with capped
   exponential backoff after a disconnect; leaves when the room empties. Requires
   `INTERNAL_AUDIO_TOKEN`.
2. **Recording.** If `RECORDING_ENABLED=true` and the S3 settings are present, starts a
   room-composite egress to `{org_id}/{meeting_id}/{time}.mp4` (`.ogg` if audio-only) and stores
   the egress id on `meeting_rooms.egress_id`. `egress_ended` then stores the uploaded key as
   `recording_ref`.

Consent: when `tenant_settings.consent_required` is true, the tap forwards only participants
with `meeting_participants.consented_at` set (and does not start if nobody has consented), and
recording starts only if every participant has consented.

`room_finished` stops the tap and the egress.

## Where taps run: `AUDIO_TAP_MODE`

| Mode | Default when | Who runs the tap |
| --- | --- | --- |
| `in-process` | `ENVIRONMENT` is `development`/`dev`/`local`/`test` | The API process (`room_lifecycle.TAPS`). Correct with ONE replica only. |
| `worker` | everything else, including `ENVIRONMENT` unset | `python -m services.meeting_service.audio_tap_worker` (run 1..N). |

An unknown value makes the API and the worker refuse to start.

In `worker` mode the verified, deduped `room_started` webhook still starts recording egress
itself, but for the tap it only writes a trigger to Redis,
`nexus:{org_id}:tap:want:{meeting_id}` = `{org_id, meeting_id, room, room_sid}` (12 h safety
TTL). `room_finished` deletes it (compare-and-delete on `room_sid`, so a late event for an
old room instance cannot cancel the new one) and stops egress. No new topic or Avro change.

Each worker polls the triggers every 2 s and claims a free room with
`SET nexus:{org_id}:tap:{meeting_id} <worker_id> NX PX TAP_CLAIM_TTL_SECONDS` (default 30), so
exactly one worker taps a room. The owner refreshes the TTL on every poll. A crashed worker's
claim expires and another worker re-claims the room on its next poll; a worker that finds its
claim gone or taken stops its tap. On SIGTERM a worker stops its taps and releases its claims
for immediate takeover. Consent gate and `INTERNAL_AUDIO_TOKEN` checks are the same code path
as in-process (`room_lifecycle.start_audio_tap`, gate reloaded at claim time). A tap refused
by the gate, or one that ends because the room emptied, drops its trigger and is not retried
until the next `room_started`, matching in-process behaviour. `METRICS_PORT` exposes the
worker's metrics.

## `/internal/audio` wire contract

- One WebSocket per (meeting, participant track); several per meeting run at once.
- Header `X-Internal-Auth: $INTERNAL_AUDIO_TOKEN`. No query parameters.
- First frame (text):
  `{"org_id": "...", "meeting_id": "...", "participant_identity": "<users.id>", "sample_rate": 16000, "encoding": "pcm_s16le"}`
- Then binary frames: raw PCM s16le, 16 kHz, mono (about 10 ms per frame). A reconnect sends
  the hello again.

## Bucket lifecycle

```bash
uv run python -m services.meeting_service.bucket_lifecycle --tenant-retention tenants.json --dry-run
```

`tenants.json` is `{org_id: days | null}` (`null` means `--default-retention-days`, 365). Drop
`--dry-run` to apply it with the `S3_*` settings. The apply replaces the bucket's whole
lifecycle configuration.

## Environment

`LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`, `INTERNAL_AUDIO_TOKEN`,
`TRANSCRIPTION_AUDIO_URL`, `AUDIO_TAP_BACKOFF_BASE_SECONDS`, `AUDIO_TAP_BACKOFF_CAP_SECONDS`,
`AUDIO_TAP_MODE`, `TAP_CLAIM_TTL_SECONDS`, `METRICS_PORT`,
`RECORDING_ENABLED`, `RECORDING_BUCKET`, `RECORDING_AUDIO_ONLY`, `RECORDING_COLD_STORAGE_CLASS`,
`S3_ENDPOINT_URL`, `S3_REGION`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`,
`S3_FORCE_PATH_STYLE`. See `../../.env.example`.

## Tests

No LiveKit account, bucket, or running transcription-service is needed for the unit tests:

```bash
uv run pytest tests/test_meeting_audio_tap.py tests/test_livekit_audio_tap.py \
  tests/test_meeting_room_lifecycle.py tests/test_meeting_transport.py \
  tests/test_audio_tap_worker.py tests/test_worker_metrics_port.py -q
```

`tests/test_livekit_webhook.py` needs the Postgres test container.
