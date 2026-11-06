# Metrics catalogue (canonical)

Source of truth for Prometheus metric names. Python: `libs/shared/metrics.py` (identical copy in
`mytheus-backend` and `mytheus-intelligence`). Node: `mytheus-realtime/services/realtime-gateway/src/metrics.ts`.

Every series carries `service` (the `create_app` name, `SERVICE_NAME`, or the worker name).
**Never** labelled by `org_id`, `user_id`, meeting id or raw path (cardinality + privacy).

## Exposure
- HTTP services: `GET /metrics` on the service port, mounted by `create_app`. Returns 404 when the
  request carries `X-Forwarded-For`, `X-Forwarded-Host`, `Forwarded` or `CF-Connecting-IP`
  (i.e. came through ingress). Scrape service-to-service on the internal network only.
- api-gateway: `/metrics` is not in `ROUTE_MAP` (never proxied) and not in `PUBLIC_PATHS`, so the
  gateway's own endpoint requires a bearer token via `edge_auth`. Prometheus cannot scrape it
  through `edge_auth` today; Grafana/ops must either scrape the gateway on a separate internal
  port or allow `/metrics` in `edge.PUBLIC_PATHS` once ingress strips it. Open follow-up.
- Workers (relay, Kafka consumers): set `METRICS_PORT` (e.g. 9100) to start a standalone exporter.
- realtime-gateway: `GET /metrics` on its HTTP port, same ingress-header rule.

## HTTP (all `create_app` services)
| Metric | Type | Labels |
|---|---|---|
| `mytheus_http_requests_total` | counter | service, method, route, status |
| `mytheus_http_request_duration_seconds` | histogram | service, method, route |
| `mytheus_http_requests_in_flight` | gauge | service |

`route` is the FastAPI route template (`/api/v1/meetings/{meeting_id}`); unmatched paths are `unmatched`.
`/metrics` itself is not measured.

## Kafka consumers
| Metric | Type | Labels |
|---|---|---|
| `mytheus_consumer_messages_processed_total` | counter | service, group, topic, outcome (`applied`, `duplicate`, `dlq`, `error`) |
| `mytheus_consumer_dlq_total` | counter | service, group, topic, failure_class (`DESERIALIZE`, `BUSINESS_INVALID`, `POISON`) |
| `mytheus_consumer_lag_seconds` | gauge | service, group, topic |

`consumer_lag_seconds` = now minus the record's Kafka timestamp, set when processing of a record
begins. It does not grow while a consumer is stalled; alert on the absence of
`messages_processed_total` increase as well.
Instrumented: intelligence `BaseConsumer` (embedding, ingestion, search), backend `core_api` and
`meeting_service` consumers. `notification_service` consumer is still a stub (not instrumented).

## Outbox relay
| Metric | Type | Labels |
|---|---|---|
| `mytheus_outbox_backlog` | gauge | service (unpublished rows after last drain) |
| `mytheus_outbox_published_total` | counter | service |
| `mytheus_outbox_poisoned_total` | counter | service (rows dead-lettered after MAX_ATTEMPTS) |

## Embedding
| Metric | Type | Labels |
|---|---|---|
| `mytheus_embedding_batch_duration_seconds` | histogram | service |
| `mytheus_embedding_batch_size` | histogram | service |

Hooks: `observe_embedding_batch(seconds, size)` / `time_embedding_batch(size)`; wired in
`EmbeddingConsumer.apply`.

## Realtime gateway (Node, prom-client)
| Metric | Type | Labels |
|---|---|---|
| `mytheus_realtime_connected_sockets` | gauge | service |
| default Node process/GC metrics (`process_*`, `nodejs_*`) | various | service |
