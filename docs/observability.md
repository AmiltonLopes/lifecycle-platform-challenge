# Part 5 — Observability Design

## Datadog Metrics and Alerts

The pipeline needs telemetry at four layers. At the infrastructure layer, I'd track `airflow.dag.duration` and set a p95 alert if it exceeds 2.5 hours — giving a 30-minute buffer before the 3-hour SLA fires. `airflow.task.duration` broken out per task helps surface whether the bottleneck is the audience build (BigQuery slot contention), the send loop (ESP latency), or the validation step (unexpectedly slow count query).

At the business logic layer, `lifecycle.campaign.audience_size` is published as a gauge on each run. An anomaly alert fires when the value falls outside ±40% of the 7-day rolling average — this catches both upstream data pipeline regressions (audience drops to near zero) and accidental audience explosions from schema or filter changes. For ESP interaction, I'd track `lifecycle.esp.requests_total` by status code, `lifecycle.esp.error_rate`, and `lifecycle.esp.429_rate` as separate metrics. The error rate alert fires at 5% over a 10-minute window. The 429 rate is tracked independently because it indicates a different operational issue — throughput limit exhaustion — which has a different response (tuning backoff parameters, negotiating higher API quota) versus a generic error spike which points to a code or infrastructure bug. A sustained 429 rate above 20% warrants a separate alert, not just a contribution to the overall error rate.

`lifecycle.esp.batch_latency_p50` and `lifecycle.esp.batch_latency_p95` capture batch round-trip time distribution. If the ESP starts degrading, per-batch latency climbs before the total DAG duration SLA fires — it's an early warning signal that lets you page before the campaign is already behind schedule. Alert on p95 crossing 3× the 7-day baseline.

`lifecycle.campaign.dedup_skipped_ratio` (skipped / total audience) is worth tracking as a standalone metric. An unexpectedly high ratio — say above 30% for a campaign that wasn't re-run — means more renters were already in the sent log than expected. That could indicate a previous run sent more than intended, the log wasn't scoped to the correct campaign+date, or a partial run completed silently. It's a signal that the dedup mechanism is doing something surprising, not just working correctly.

`lifecycle.campaign.send_completion_rate` (sent / eligible audience) is the end-to-end signal — anything below 95% warrants a page.

## Detecting and Preventing Double-Sends

The primary mechanism is the incremental `sent_renters.json` log. Because the file is written after each successful batch (not at the end of the full run), a crash or retry mid-pipeline means only the in-flight batch at the moment of failure could theoretically be re-sent. All completed batches are already recorded and will be skipped.

For production hardening, the file-based log is replaced with a BigQuery table `marketing.campaign_send_dedup` with a composite key of `(campaign_id, renter_id, send_date)`. The pipeline checks this table before each batch rather than reading a local file, which works across Airflow workers and survives pod restarts. To close the remaining race window (process dies after ESP success but before the BQ write), the ESP call should include an idempotency key — typically `sha256(campaign_id + renter_id + send_date)`. Most modern ESPs treat duplicate keys as no-ops. At the DAG level, setting `max_active_runs=1` prevents two concurrent runs from interleaving their batch writes and producing duplicate sends from a race condition.

## ESP Down Mid-Send and Circuit Breaker

Because the sent log is updated incrementally, partial progress survives any restart. If the ESP goes down mid-run, the current batch fails all retries, is logged with the full `renter_id` list, and the loop continues with subsequent batches. When the ESP recovers, a task-level re-trigger from the Airflow UI resumes cleanly — the dedup log ensures already-sent batches are skipped and only the failed ones are retried.

The missing piece in the base implementation is a circuit breaker: after three consecutive `False` returns from `_send_with_retry`, the pipeline should raise an exception and fail the task immediately rather than spending retries on every remaining batch against a downed API. The consecutive-failure counter resets on any success, so transient errors don't trip the breaker. The failure threshold is stored in an Airflow Variable so it can be tuned without a deploy. When the task fails, the Airflow `on_failure_callback` fires a PagerDuty alert. The failed `renter_id` lists in structured logs provide the exact payload for a manual re-send if the ops team decides to process them out of band while the ESP incident is resolved.
