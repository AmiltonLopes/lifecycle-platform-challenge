# AI Session Log — Lifecycle Platform Challenge

**Tool:** Claude (claude.ai)
**Session date:** 2026-04-29 / 2026-04-30

---

**Human:** got a 48h take-home. going to use you to pressure-test my decisions, not to write the code. starting with the SQL for Part 1. here's what I have:

```sql
WITH search_counts AS (
    SELECT
        renter_id,
        COUNT(*) AS search_count
    FROM renter_activity
    WHERE
        event_type = 'search'
        AND event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    GROUP BY renter_id
    HAVING COUNT(*) >= 3
)
SELECT
    rp.renter_id,
    rp.email,
    rp.phone,
    rp.last_login,
    sc.search_count,
    DATE_DIFF(CURRENT_DATE(), DATE(rp.last_login), DAY) AS days_since_login
FROM renter_profiles rp
INNER JOIN search_counts sc ON rp.renter_id = sc.renter_id
LEFT JOIN suppression_list sl ON rp.renter_id = sl.renter_id
WHERE
    rp.subscription_status = 'churned'
    AND rp.last_login < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    AND rp.phone IS NOT NULL
    AND rp.sms_consent = TRUE
    AND (rp.dnd_until IS NULL OR rp.dnd_until < CURRENT_TIMESTAMP())
    AND sl.renter_id IS NULL
```

any issues with this?

**Assistant:** Logic is correct. Two things worth checking at production scale:

The `suppression_list` schema has no uniqueness constraint on `renter_id`. If a renter appears more than once there — which happens when they get suppressed, re-subscribe, and get suppressed again — the LEFT JOIN multiplies rows and you get duplicate renters in the output. Deduplicating the suppression side before joining is the cleaner fix.

On the CTE: it scans `renter_activity` for the full 90-day window across all event types before filtering to `event_type = 'search'`. How is that table partitioned? If it's not on `event_timestamp`, the query is scanning more data than it needs to.

**Human:** partitioned on event_timestamp so the filter prunes it. on the duplicates — would NOT EXISTS be more appropriate than LEFT JOIN + IS NULL here? I've seen arguments both ways at scale.

**Assistant:** In BigQuery specifically, the query planner typically rewrites both to an anti-join internally, so at scale the execution plan ends up equivalent. The difference is really about readability and intent. `LEFT JOIN + IS NULL` is idiomatic in SQL and widely understood. `NOT EXISTS` reads more like English but is slightly more verbose. For an audience pipeline where someone else might need to maintain it, I'd stay with `LEFT JOIN` — the DISTINCT inside a deduplicated CTE is the important part, not which anti-join pattern you pick.

**Human:** agreed, keeping LEFT JOIN. I'm relying on CURRENT_TIMESTAMP() for idempotency — running the same query twice on the same day returns the same result as long as data doesn't change. is that the right read or is there a case where it breaks?

**Assistant:** That's correct for BigQuery — `CURRENT_TIMESTAMP()` is evaluated once at query start and used consistently throughout execution. The idempotency holds as long as the underlying tables don't change between runs, which is the assumption the spec is making. The case where it breaks is a backfill: if you need to reproduce the audience for a past date, `CURRENT_TIMESTAMP()` won't cooperate — you'd need a `@run_date` parameter for that. But for the daily operational case they're describing, you're fine.

**Human:** not building for backfills here so CURRENT_TIMESTAMP() stays. what about the WRITE_TRUNCATE when I export to staging — does that create any risk window where the table is empty between runs?

**Assistant:** Yes, there's a brief window. BigQuery's `WRITE_TRUNCATE` is not atomic in the sense that it first truncates, then writes — so if you query the staging table during that window (e.g., a monitoring query or a concurrent DAG run) you'll get zero rows. In practice this is rarely an issue because BigQuery jobs are fast and the window is milliseconds. If it concerned you, `WRITE_TRUNCATE` to a temp table and then a `MERGE` or table copy to the staging table would be atomic — but that's over-engineering for this scope.

**Human:** noted, acceptable risk for this. moving to Part 2. here's the full sender:

```python
import json
import logging
import random
import time
from pathlib import Path

logger = logging.getLogger(__name__)

BATCH_SIZE = 100
MAX_RETRIES = 5
BASE_BACKOFF = 1.0


def _load_sent_ids(path: str) -> set:
    p = Path(path)
    if not p.exists():
        return set()
    with p.open() as f:
        return set(json.load(f).get("sent_ids", []))


def _persist_sent_ids(path: str, sent_ids: set) -> None:
    with open(path, "w") as f:
        json.dump({"sent_ids": sorted(sent_ids)}, f)


def _backoff(attempt: int) -> None:
    delay = BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 1)
    logger.warning("Backing off %.2fs (attempt %d/%d)", delay, attempt + 1, MAX_RETRIES)
    time.sleep(delay)


def _send_with_retry(esp_client, campaign_id: str, batch: list[dict]) -> bool:
    for attempt in range(MAX_RETRIES):
        try:
            resp = esp_client.send_batch(campaign_id, batch)
        except Exception:
            logger.exception("Exception calling ESP on attempt %d", attempt + 1)
            if attempt < MAX_RETRIES - 1:
                _backoff(attempt)
            continue

        if resp.status_code == 200:
            return True
        if resp.status_code == 429:
            _backoff(attempt)
            continue

        logger.error("ESP returned %d on attempt %d: %s",
                     resp.status_code, attempt + 1, resp.json())
        return False

    return False


def execute_campaign_send(
    campaign_id: str,
    audience: list[dict],
    esp_client,
    sent_log_path: str = "sent_renters.json",
) -> dict:
    start = time.monotonic()
    sent_ids = _load_sent_ids(sent_log_path)
    total_sent = 0
    total_failed = 0
    total_skipped = 0

    eligible = []
    for renter in audience:
        if renter["renter_id"] in sent_ids:
            total_skipped += 1
        else:
            eligible.append(renter)

    logger.info("Campaign %s: %d total, %d eligible, %d already sent",
                campaign_id, len(audience), len(eligible), total_skipped)

    for i in range(0, len(eligible), BATCH_SIZE):
        batch = eligible[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        batch_ids = [r["renter_id"] for r in batch]
        logger.info("Sending batch %d (%d recipients)", batch_num, len(batch))

        if _send_with_retry(esp_client, campaign_id, batch):
            sent_ids.update(batch_ids)
            total_sent += len(batch)
            _persist_sent_ids(sent_log_path, sent_ids)
        else:
            total_failed += len(batch)
            logger.error("Batch %d failed after %d retries — renter_ids: %s",
                         batch_num, MAX_RETRIES, batch_ids)

    summary = {
        "total_sent": total_sent,
        "total_failed": total_failed,
        "total_skipped": total_skipped,
        "elapsed_seconds": round(time.monotonic() - start, 2),
    }
    logger.info("Campaign %s complete: %s", campaign_id, summary)
    return summary
```

**Assistant:** This is solid. The incremental persist after each successful batch is the right decision — crash recovery without re-sending everything.

A few things:

The backoff formula tops out at ~16s before the 5th attempt. That's reasonable, but if the ESP is recovering from an outage, 16 seconds might not be enough window. Adding a cap like `min(BASE_BACKOFF * (2 ** attempt), 60.0)` and maybe pushing `MAX_RETRIES` to 6 or 7 gives more room. Not required for this scope, but worth a note.

On the 5xx handling — returning False immediately without retrying is a design decision that deserves a comment in the README. Some teams retry 5xx with a longer initial delay, others don't. Your current approach is defensible and I'd expect that question in the technical review.

Memory-wise: you're loading the entire audience into a list before batching. If the audience is very large — tens of thousands — this is fine. But for campaigns at 30M user scale, if the audience were ever a significant fraction of that, you'd want to stream from BigQuery row by row using a generator rather than materializing everything into memory. Not an issue here given the filter criteria would limit the audience to a much smaller subset, but worth knowing.

**Human:** the 5xx decision was intentional — my reasoning is that ESPs return 5xx when they're having issues, retrying immediately just adds load to an already struggling service. if I wanted 5xx retry I'd treat it separately with a longer initial delay and fewer attempts. I'll document that in the README. on memory — the audience for this campaign type would realistically be in the low hundreds of thousands at most, so materializing is fine. what concerns me more is the sent_log growing unbounded over time across runs.

**Assistant:** That's a real issue in production. The file grows by `audience_size` per run and you never prune it. After weeks of daily runs you'd be loading and deserializing a multi-MB JSON file on every execution, and the set intersection check grows linearly. The pragmatic fix is scoping the log to `campaign_id + date` — so instead of one ever-growing file, each day gets its own log. The dedup check then only needs to look at the current day's file, which is always small. Longer term the right answer is a BigQuery table with `(campaign_id, renter_id, send_date)` as the composite key — you noted that in the README which is the right call.

**Human:** yeah, that's already in the README. day-scoped file is a good intermediate approach, I'll mention it too. on the backoff — with 5 retries and no cap, what's the actual worst case total wait time if every attempt hits 429?

**Assistant:** With `BASE_BACKOFF = 1.0` and jitter between 0 and 1 second: attempt 0 waits ~1–2s, attempt 1 ~2–3s, attempt 2 ~4–5s, attempt 3 ~8–9s, attempt 4 ~16–17s. Total worst case across all retries before the final failure is roughly 31–36 seconds per batch. With 100 recipients per batch and say 500 batches (50,000 renters), if every single batch hits max retries that's ~4–5 hours of backoff alone. In practice you wouldn't sustain that across all batches, but it illustrates why a cap matters at scale. For this challenge scope the numbers are fine.

**Human:** fair, noting the cap as a production improvement. moving to Part 3. here's the DAG with the full task bodies:

```python
from datetime import datetime, timedelta
import logging
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)

CAMPAIGN_ID = "sms-reactivation"
AUDIENCE_TABLE = "marketing.sms_reactivation_audience_staging"
RESULTS_TABLE = "marketing.campaign_send_log"
BASELINE_AUDIENCE_SIZE = 50_000
MAX_AUDIENCE_MULTIPLIER = 2.0


def _on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    logger.error("SLA missed for %s — blocked by: %s",
                 dag.dag_id, [ti.task_id for ti in blocking_tis])


@dag(
    dag_id="sms_reactivation_campaign",
    schedule_interval="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "lifecycle",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "sla": timedelta(hours=3),
    },
    sla_miss_callback=_on_sla_miss,
    tags=["lifecycle", "sms"],
)
def sms_reactivation_campaign():

    @task()
    def build_audience() -> int:
        from google.cloud import bigquery
        sql_path = os.path.join(os.path.dirname(__file__), "..", "sql", "audience_query.sql")
        with open(sql_path) as f:
            audience_sql = f.read()
        client = bigquery.Client()
        table_ref = client.dataset("marketing").table("sms_reactivation_audience_staging")
        job_config = bigquery.QueryJobConfig(
            destination=table_ref,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        client.query(audience_sql, job_config=job_config).result()
        result = next(client.query(f"SELECT COUNT(*) AS cnt FROM `{AUDIENCE_TABLE}`").result())
        return int(result.cnt)

    @task()
    def validate_audience(count: int) -> int:
        if count == 0:
            raise ValueError("Audience is empty — aborting to prevent a silent no-op send")
        ceiling = BASELINE_AUDIENCE_SIZE * MAX_AUDIENCE_MULTIPLIER
        if count > ceiling:
            raise ValueError(
                f"Audience size {count:,} exceeds {ceiling:,.0f} "
                f"({MAX_AUDIENCE_MULTIPLIER}x baseline). Manual review required."
            )
        logger.info("Audience validated: %d renters", count)
        return count

    @task()
    def execute_send(count: int) -> dict:
        from google.cloud import bigquery
        from pipeline.campaign_sender import execute_campaign_send
        from pipeline.esp_client import ESPClient
        client = bigquery.Client()
        rows = client.query(f"SELECT renter_id, email, phone FROM `{AUDIENCE_TABLE}`").result()
        audience = [dict(row) for row in rows]
        return execute_campaign_send(
            campaign_id=CAMPAIGN_ID,
            audience=audience,
            esp_client=ESPClient(),
            sent_log_path="/tmp/sent_renters.json",
        )

    @task()
    def log_and_notify(summary: dict) -> None:
        import requests
        from google.cloud import bigquery
        ctx = get_current_context()
        client = bigquery.Client()
        errors = client.insert_rows_json(RESULTS_TABLE, [{
            "campaign_id": CAMPAIGN_ID,
            "run_date": ctx["ds"],
            **summary,
        }])
        if errors:
            logger.error("BQ insert errors: %s", errors)
        webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
        if webhook_url:
            text = (
                f":white_check_mark: *SMS Reactivation* `{ctx['ds']}`\n"
                f"Sent: {summary['total_sent']:,} | "
                f"Failed: {summary['total_failed']:,} | "
                f"Skipped: {summary['total_skipped']:,} | "
                f"Duration: {summary['elapsed_seconds']}s"
            )
            requests.post(webhook_url, json={"text": text}, timeout=10)

    count = build_audience()
    validated = validate_audience(count)
    summary = execute_send(validated)
    log_and_notify(summary)


sms_reactivation_campaign()
```

**Assistant:** This is well structured. The `max_active_runs=1` is important given the file-based dedup — good that it's there.

A few observations:

In `execute_send`, you're loading the full audience from the staging table into memory again after `build_audience` already materialized it. If you're already storing the count in XCom, you could store the audience itself — but XCom in Airflow is not designed for large payloads (it's backed by the metadata DB). The current pattern of re-querying from the staging table is actually the right approach at scale. Just flagging that it's a deliberate choice, not a missed optimization.

The `BASELINE_AUDIENCE_SIZE` constant — in production this would drift from reality over time. An Airflow Variable or a query against a `campaign_send_log` rolling average would be more robust. Worth mentioning as a known limitation.

Also: `@task` passes the return value of `execute_send` directly to `log_and_notify` via XCom. The summary dict has four keys, all primitives — that's fine. If it ever grows to include large payloads (e.g., a list of failed renter_ids for downstream processing), you'd need to write those to GCS and pass a URI instead.

**Human:** the XCom size point is something I already had in the back of my mind. the summary dict staying small is intentional — failed renter_ids are in the logs, not passed via XCom. on BASELINE_AUDIENCE_SIZE — I documented it as a known limitation in the README, pointing to using a rolling query instead. what's your take on using @task over PythonOperator here — is there a scenario where PythonOperator would be the better call?

**Assistant:** `@task` is the better choice for this use case. PythonOperator would be preferable in a few specific scenarios: if you need the task to be defined dynamically (e.g., generating tasks in a loop based on a config), or if you're on Airflow 1.x. For TaskFlow API in Airflow 2.x, `@task` gives you automatic XCom serialization of return values, cleaner code structure, and better type annotation support. The one trade-off is that `@task` makes it slightly harder to introspect XCom values from the Airflow UI since they're not explicitly named — with PythonOperator you'd call `ti.xcom_push(key="my_key", value=...)` and can look up by key. Minor concern for operational visibility but worth knowing.

**Human:** good to know. moving to Part 4. I want to pressure-test the sensor design for model freshness — I wrote a `@task.sensor` that polls every 2 minutes for up to an hour. what's the risk if the DAG and the scoring job are both scheduled around the same time?

**Assistant:** The risk is a race condition on the very first poll — if the DAG starts at 5:00 AM and the scoring job finishes at 5:01 AM, the sensor's first poke at 5:00 AM will return False, but the second poke at 5:02 AM will return True and unblock the DAG. That's fine. The real risk is scheduling drift over time: if the scoring job gradually gets slower and starts finishing at 5:50 AM instead of 5:00 AM, you lose your safety margin. A timeout of 1 hour means the sensor fails at 6:00 AM, which still gives the campaign DAG time to complete before the 8:00 AM SLA — but only just. In production I'd set the sensor timeout to 90 minutes and log a warning metric when it takes longer than 30 minutes, so you can see the drift before it becomes a problem.

**Human:** that's a good point about drift. I'll note the 90-minute recommendation. the other thing I'm unsure about is the fallback strategy — I wrote three options: fail hard, send without scores, skip the run. is the framing complete or am I missing a fourth option?

**Assistant:** There's a fourth worth mentioning: partial fallback. Instead of choosing between "send to everyone without scores" and "send to nobody," you could send only to renters whose scores are available from the previous day's run — treating stale scores as better than no scores for high-confidence cases (e.g., `predicted_conversion_probability >= 0.7`). This is a niche option but it shows you've thought about the spectrum between all-or-nothing. Whether it's appropriate depends on model volatility — if scores change significantly day to day, stale scores might be worse than no scores. Worth a sentence in the doc.

**Human:** makes sense, adding that as an option with the caveat about model volatility. last thing on Part 5 — the observability section. I have audience_size anomaly detection, ESP error rate alerts, DAG duration, and send_completion_rate. am I missing any metrics that would matter in production?

**Assistant:** A few that are often overlooked:

`esp.429_rate` as a standalone metric, not just folded into error rate. A sustained 429 rate tells you specifically that you're hitting throughput limits, which has a different operational response than a generic error rate spike — you'd tune the backoff or negotiate a higher API quota with the ESP, not investigate a code bug.

Per-batch latency distribution (p50, p95 of batch round-trip time). If the ESP starts degrading, average send time per batch goes up, which cascades into total campaign duration before it shows up as a completion alert. It's an early warning signal.

`dedup.skipped_ratio` — what fraction of the audience was skipped because it was already in the sent log. If this number is unexpectedly high, it means a previous run sent more than expected, or the log wasn't reset between campaigns. Helps catch silent re-run issues.

And if you're using Datadog APM traces on the Airflow tasks, task-level trace data gives you the full picture without needing to instrument each metric separately.

**Human:** the 429_rate separately is a good call — I had it bundled with error_rate which is not the same thing operationally. and per-batch latency is something I wouldn't have thought to add. updating the observability doc with both. done.
