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
    logger.warning("Rate limited — backing off %.2fs (attempt %d/%d)", delay, attempt + 1, MAX_RETRIES)
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

        logger.error(
            "ESP returned %d on attempt %d: %s",
            resp.status_code, attempt + 1, resp.json(),
        )
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

    logger.info(
        "Campaign %s — %d total, %d eligible, %d already sent",
        campaign_id, len(audience), len(eligible), total_skipped,
    )

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
            logger.error(
                "Batch %d failed after %d retries — renter_ids: %s",
                batch_num, MAX_RETRIES, batch_ids,
            )

    summary = {
        "total_sent": total_sent,
        "total_failed": total_failed,
        "total_skipped": total_skipped,
        "elapsed_seconds": round(time.monotonic() - start, 2),
    }
    logger.info("Campaign %s complete: %s", campaign_id, summary)
    return summary
