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
LEFT JOIN suppressed sl ON rp.renter_id = sl.renter_id
WHERE
    rp.subscription_status = 'churned'
    AND rp.last_login < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    AND rp.phone IS NOT NULL
    AND rp.sms_consent = TRUE
    AND (rp.dnd_until IS NULL OR rp.dnd_until < CURRENT_TIMESTAMP())
    AND sl.renter_id IS NULL
