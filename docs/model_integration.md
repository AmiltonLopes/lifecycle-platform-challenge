# Part 4 — Value Model Integration

## Modifying the BigQuery Query

The cleanest integration point is an additional `INNER JOIN` on `ml_predictions.renter_send_scores` added after the existing joins in the Part 1 query. Using `INNER JOIN` (rather than `LEFT JOIN`) naturally enforces the "must have a score to be included" requirement — renters who weren't scored are silently excluded, which is the correct behavior.

The conversion threshold is introduced as a query parameter (`@conversion_threshold`) rather than a hardcoded literal. This keeps the SQL logic stable across campaigns while allowing per-segment tuning at execution time without touching the query itself.

The six-week second model changes the design in one place: the scores table gets a `segment_id` column (or the DAG passes a `@model_version` parameter that maps to a segment). The join condition gains `AND scores.segment_id = @segment_id`. The overall query structure stays identical — only the parameter values differ per segment run.

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
),
suppressed AS (
    SELECT DISTINCT renter_id
    FROM suppression_list
),
scored_audience AS (
    SELECT
        rp.renter_id,
        rp.email,
        rp.phone,
        rp.last_login,
        sc.search_count,
        DATE_DIFF(CURRENT_DATE(), DATE(rp.last_login), DAY) AS days_since_login
    FROM renter_profiles rp
    INNER JOIN search_counts sc ON rp.renter_id = sc.renter_id
    INNER JOIN ml_predictions.renter_send_scores scores
        ON rp.renter_id = scores.renter_id
        AND scores.model_version = @model_version
        AND DATE(scores.scored_at) = CURRENT_DATE()
        AND scores.predicted_conversion_probability >= @conversion_threshold
    LEFT JOIN suppressed sl ON rp.renter_id = sl.renter_id
    WHERE
        rp.subscription_status = 'churned'
        AND rp.last_login < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        AND rp.phone IS NOT NULL
        AND rp.sms_consent = TRUE
        AND (rp.dnd_until IS NULL OR rp.dnd_until < CURRENT_TIMESTAMP())
        AND sl.renter_id IS NULL
)
SELECT * FROM scored_audience
```

`@model_version` and `@conversion_threshold` are passed as BigQuery query parameters by the Airflow task. A config table (`lifecycle.model_segment_config`) mapping `segment_id → model_version, threshold` keeps the DAG parameter-free for most changes.

## Adding Model Freshness Dependency to the DAG

A sensor task is inserted before `build_audience`. It polls every two minutes for up to one hour checking whether today's scores exist:

```python
@task.sensor(poke_interval=120, timeout=3600, mode="reschedule")
def wait_for_model_scores() -> PokeReturnValue:
    from google.cloud import bigquery
    client = bigquery.Client()
    result = client.query("""
        SELECT COUNT(*) AS cnt
        FROM ml_predictions.renter_send_scores
        WHERE DATE(scored_at) = CURRENT_DATE()
          AND model_version = @model_version
    """).result()
    count = next(result).cnt
    return PokeReturnValue(is_done=count > 0)
```

Updated task chain:

```
wait_for_model_scores >> build_audience >> validate_audience >> execute_send >> log_and_notify
```

Using `mode="reschedule"` avoids holding a worker slot during the wait window, which matters at scale.

## Handling a Late or Missing Scoring Job

The sensor times out after one hour (configurable via Airflow Variable). With two task-level retries, the DAG will wait up to ~three hours total before hard-failing. This covers the common case where the scoring job is delayed but eventually arrives.

When it doesn't arrive, three strategies are on the table — the right choice is a business decision, not a technical one:

**Wait and hard-fail (default).** The DAG fails, PagerDuty fires, the team investigates. No SMS goes out that day. This is the safest default because sending to unscored users wastes volume budget and can suppress high-intent users who would have converted anyway with a targeted message.

**Fallback to unscored send.** If the team decides partial coverage is better than no coverage, an Airflow Variable (`allow_unscored_fallback = true`) switches the `build_audience` task to run a version of the query without the model JOIN. This should be an explicit opt-in, never a silent default. A Slack alert accompanies every fallback run so the lifecycle team is aware.

**Skip and wait for next scheduled run.** Clean failure mode when the campaign cadence allows for it. The next day's run will have fresh scores. Works well for weekly campaigns; risky for daily ones where missing a day has measurable revenue impact.

**Partial send using previous day's scores.** A middle path between full fallback and no send: filter to renters whose stale scores are above a higher confidence threshold (e.g., `predicted_conversion_probability >= 0.7` instead of the usual `@conversion_threshold`). The reasoning is that high-confidence predictions are more stable day-to-day and less likely to be invalidated by a single scoring delay. This option is worth considering when the scoring job is occasionally late but the model itself doesn't exhibit high daily volatility. If model scores shift significantly between days — common in models retrained on recent behavioral data — stale high-confidence scores may still be misleading and this option becomes worse than skipping entirely.

The choice should live in Airflow Variables so ops can change behavior without a code deploy.
