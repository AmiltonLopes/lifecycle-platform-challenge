# lifecycle-platform-challenge

Take-home project for the Senior Lifecycle Platform Engineer role.

## Structure

```
.
├── sql/
│   └── audience_query.sql        # Part 1 — BigQuery audience segmentation
├── pipeline/
│   └── campaign_sender.py        # Part 2 — ESP send pipeline
├── dags/
│   └── reactivation_dag.py       # Part 3 — Airflow DAG
├── docs/
│   ├── model_integration.md      # Part 4 — ML model integration design
│   └── observability.md          # Part 5 — Observability and recovery design
└── ai-session/
    └── claude-log.md             # AI assistance session log
```

## How to Run Locally

**Requirements:** Python 3.11+, Google Cloud SDK authenticated (`gcloud auth application-default login`), BigQuery project with the schema from the challenge spec.

```bash
pip install google-cloud-bigquery apache-airflow apache-airflow-providers-google requests
```

### SQL (Part 1)

```bash
bq query --use_legacy_sql=false --project_id=YOUR_PROJECT < sql/audience_query.sql
```

### Python Pipeline (Part 2)

`ESPClient` is an injectable dependency — it's provided by the platform, not implemented here. For local testing:

```python
from unittest.mock import MagicMock
from pipeline.campaign_sender import execute_campaign_send

mock_esp = MagicMock()
mock_esp.send_batch.return_value = MagicMock(status_code=200, json=lambda: {})

audience = [
    {"renter_id": "r1", "email": "user@example.com", "phone": "+15551234567"},
]

result = execute_campaign_send("test-campaign-001", audience, mock_esp)
print(result)
```

To test the 429 backoff path:

```python
mock_esp.send_batch.side_effect = [
    MagicMock(status_code=429, json=lambda: {}),
    MagicMock(status_code=429, json=lambda: {}),
    MagicMock(status_code=200, json=lambda: {}),
]
```

### Airflow DAG (Part 3)

```bash
export AIRFLOW_HOME=~/airflow
export SLACK_WEBHOOK_URL=https://hooks.slack.com/...  # optional
airflow db init
airflow dags test sms_reactivation_campaign 2024-01-15
```

The DAG imports `from pipeline.esp_client import ESPClient` — place the provided `ESPClient` implementation at `pipeline/esp_client.py` before running.

## Assumptions

- BigQuery project is set via the environment default; dataset names use `marketing` for staging/results and `ml_predictions` for model scores (Part 4).
- `ESPClient` is injected as a dependency. The reference at `pipeline/esp_client.py` is a placeholder for the actual provided implementation.
- `BASELINE_AUDIENCE_SIZE = 50_000` in the DAG would be replaced with a query against a historical averages table in production.
- Airflow is running on Cloud Composer or a managed service with the Google provider pre-installed.
- The sent log at `/tmp/sent_renters.json` is sufficient for this scope. Production approach is covered in `docs/observability.md`.
- Model scores are assumed available before 5:00 AM UTC. The sensor design in `docs/model_integration.md` covers the late-arrival case.

## Design Decisions

**SQL — single-pass query with CTE:** All logic lives in one query with a `search_counts` CTE. This keeps the audience build atomic — no partial state if the job fails mid-way — and makes the intent of each filter clause easy to read independently.

**Suppression via LEFT JOIN + IS NULL:** Preferred over `NOT IN` because `NOT IN` silently returns zero rows when the subquery contains any NULLs. `LEFT JOIN + IS NULL` is explicit and performs well at scale when `renter_id` is indexed on the suppression table.

**Incremental sent log writes:** The log is flushed to disk after each successful batch, not at the end of the run. A crash between batch N and N+1 means at worst batch N is re-attempted on the next run — acceptable idempotency behavior and much better than re-sending the entire campaign.

**File-based dedup over a database:** Keeps the implementation self-contained for this challenge without requiring additional infrastructure. The trade-off is that the file is local to the worker and won't survive a pod migration in a distributed Airflow setup. The production upgrade path is covered in `docs/observability.md`.

**`@task` decorator over `PythonOperator`:** XCom passing is automatic (return values are serialized transparently), the task chain reads linearly, and Python type hints are preserved. `PythonOperator` would work equally well but requires more boilerplate for XCom pulls.

## What I'd Do Differently With More Time

- Replace `sent_renters.json` with a BigQuery dedup table (`campaign_id`, `renter_id`, `send_date` composite key) to support cross-worker idempotency and auditability.
- Add a unit test suite: mock-ESP tests for the full retry matrix (200, 429 × N then 200, consecutive failures), dedup logic, and empty audience handling.
- Extract all constants (table names, thresholds, schedule) into Airflow Variables or a config file so they can be changed without a code deploy.
- Add a circuit breaker to the send loop: fail the task fast after N consecutive batch failures rather than continuing to call a downed ESP through the entire audience.
- Publish custom Datadog metrics directly from the pipeline using `datadog` SDK rather than relying on log scraping.
- Parameterize the SQL fully (`@run_date`, `@model_version`, `@conversion_threshold`) and pass them from the DAG via BigQuery query parameters for cleaner separation of configuration from logic.
