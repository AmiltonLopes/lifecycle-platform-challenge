import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)

CAMPAIGN_ID = "sms-reactivation"
AUDIENCE_TABLE = "marketing.sms_reactivation_audience_staging"
RESULTS_TABLE = "marketing.campaign_send_log"
BASELINE_AUDIENCE_SIZE = 50_000
MAX_AUDIENCE_MULTIPLIER = 2.0


def _on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    logger.error(
        "SLA missed for %s — still blocked by: %s",
        dag.dag_id,
        [ti.task_id for ti in blocking_tis],
    )


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
        count = int(result.cnt)
        logger.info("Audience built: %d renters", count)
        return count

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
