# Operational notes

## Start, pause, resume and inspect

`POST /queue/drain` prevents new claims while allowing in-flight work to finish. It is a shared PostgreSQL flag, so all workers observe the same setting. `POST /queue/resume` restores claiming. `POST /queue/cancel-pending` cancels current queued/retrying jobs; it does not terminate running work. Use `GET /queue/depth` to inspect ready, delayed, retry-ready, retry-wait, running, succeeded, dead-lettered and cancelled counts.

`GET /jobs/{id}/executions` exposes the attempt trail. An attempt marked `LOST` represents an expired lease and an unknown external outcome, not proof that no side effects happened. `dead_letters` retains exhausted jobs for investigation. Automatic dead-letter replay is intentionally not implemented; deliberate retries should create a new logical job after reviewing the failure.

## Prometheus metrics

Scrape `GET /actuator/prometheus` from components with network access. Metrics include:

- `jobs_queue_depth` — gauge of ready queued/retry jobs, queried from the database.
- `jobs_worker_active` — active claims handled by this process (worker component).
- `jobs_worker_utilization` — active / configured concurrency for the worker instance.
- `jobs_execution_duration_seconds` — attempt duration timer with configured p50/p95 and histogram buckets.
- `jobs_retries_total` — attempts scheduled for retry.
- `jobs_dead_letter_total` — terminal dead-letter transitions.
- `jobs_completed_total{outcome=...}` — successful and failed attempt transitions.

Metrics are **scraped**, not pushed. Protect the Spring Actuator endpoints with a VPC/firewall or monitoring gateway; when `API_KEY` is configured the Prometheus and metrics routes also require `X-API-Key`. Do not expose internal telemetry unauthenticated to the public internet. Application instances have individual in-process counters; use Prometheus aggregation across instances.

## Worker configuration

`WORKER_CONCURRENCY` bounds claimed jobs per worker process. Use `WORKER_POLL_MS` and `WORKER_RECOVERY_MS` to adjust polling and lease recovery. Job timeout is per attempt, with `WORKER_LEASE_GRACE_SECONDS` added for recovery. A handler must respect interruption; arbitrary external tasks may need process isolation.

## Scaling discussion

First measure queue-age p95, arrival rate, handler latency, DB CPU, active workers and database contention. Increase worker replicas for execution-bound workloads. Tune claim batch size and polling, add appropriate indexes, archive completed attempts, and use a connection pool. Consider partitioning/archiving historical jobs as the history grows. Sharding or RabbitMQ may become relevant only after the database queue is demonstrably the bottleneck. Read replicas can offload status requests but should not be used for transactional claiming. PostgreSQL `SKIP LOCKED` permits safe independent worker processes but does not guarantee strict global priority ordering.

## Security and retention

Set `API_KEY` on the API component to require `X-API-Key` for job, schedule and queue endpoints. This is a minimal assessment-grade protection, not a full user-authentication or tenancy system. Protect deployment, management endpoints, secrets, database credentials and metrics in cloud network policy. Use managed PostgreSQL, backups, TLS, and a retention/archival policy for old executions in a hardened deployment.
