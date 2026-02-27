#!/usr/bin/env python3
"""
Meta Ads ID Notifier - Standalone Service
==========================================

Standalone service that:
1. Reads entries from launches_v2 table (polling)
2. Fetches ad name via Meta API
3. Sends to Make.com webhook
4. Records attempt result in launches_v2_processed (UPSERT)

IMPORTANT BEHAVIOR:
- Only processes ads that haven't been successfully processed yet
- Re-processes failed attempts after configured retry interval
- Prevents unnecessary API calls by filtering out successfully processed ads

Deploy as independent Railway service.
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional

import requests


# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)


# -----------------------------
# Configuration
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL")  # PostgreSQL connection
WEBHOOK_URL = os.getenv("MAKE_WEBHOOK_URL")  # Make.com webhook
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")  # Meta API token

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))  # 60 seconds
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))  # Process 100 at a time

# Retry failed attempts (still supported)
RETRY_FAILED_AFTER_MINUTES = int(os.getenv("RETRY_FAILED_AFTER_MINUTES", "5"))

# Optional: if you want to only send the most recent N minutes each run,
# set RECENT_WINDOW_MINUTES. If unset/empty, we do not time-filter.
# Example: RECENT_WINDOW_MINUTES=10
RECENT_WINDOW_MINUTES = os.getenv("RECENT_WINDOW_MINUTES")
RECENT_WINDOW_MINUTES_INT: Optional[int] = (
    int(RECENT_WINDOW_MINUTES) if RECENT_WINDOW_MINUTES and RECENT_WINDOW_MINUTES.strip() else None
)


# -----------------------------
# Database Setup
# -----------------------------
def init_processed_table():
    """Create the processed table if it doesn't exist."""
    try:
        import psycopg2

        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS launches_v2_processed (
                launch_key TEXT PRIMARY KEY,
                processed_at TIMESTAMPTZ DEFAULT NOW(),
                campaign_id TEXT,
                adset_id TEXT,
                creative_id TEXT,
                ad_id TEXT,
                ad_name TEXT,
                webhook_status TEXT
            )
        """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_processed_at
            ON launches_v2_processed(processed_at DESC)
        """
        )

        conn.commit()
        cursor.close()
        conn.close()

        logging.info("✅ Processed table initialized")

    except Exception as e:
        logging.error(f"Failed to initialize processed table: {e}")
        raise


# -----------------------------
# Database Operations
# -----------------------------
def get_ads_from_launches_v2(limit: int = 100) -> List[Dict]:
    """
    Get ads from launches_v2 that haven't been successfully processed yet.
    Excludes ads that are already in launches_v2_processed with 'success' status.
    """
    try:
        import psycopg2
        import psycopg2.extras

        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        if RECENT_WINDOW_MINUTES_INT is not None:
            cursor.execute(
                """
                SELECT
                    l.launch_key,
                    l.campaign_id,
                    l.adset_id,
                    l.creative_id,
                    l.ad_id
                FROM launches_v2 l
                LEFT JOIN launches_v2_processed p ON l.launch_key = p.launch_key
                WHERE l.created_at > NOW() - (%s * INTERVAL '1 minute')
                  AND (p.launch_key IS NULL OR p.webhook_status = 'failed')
                ORDER BY l.created_at DESC
                LIMIT %s
                """,
                (RECENT_WINDOW_MINUTES_INT, limit),
            )
        else:
            cursor.execute(
                """
                SELECT
                    l.launch_key,
                    l.campaign_id,
                    l.adset_id,
                    l.creative_id,
                    l.ad_id
                FROM launches_v2 l
                LEFT JOIN launches_v2_processed p ON l.launch_key = p.launch_key
                WHERE p.launch_key IS NULL OR p.webhook_status = 'failed'
                ORDER BY l.created_at DESC
                LIMIT %s
                """,
                (limit,),
            )

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return [dict(row) for row in rows]

    except Exception as e:
        logging.error(f"Failed to get ads from launches_v2: {e}")
        return []


def get_failed_ads_to_retry(retry_after_minutes: int) -> List[Dict]:
    """
    Get failed ads that are old enough to retry.
    NOTE: query fixed to use a safe interval expression.
    """
    try:
        import psycopg2
        import psycopg2.extras

        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute(
            """
            SELECT
                launch_key,
                campaign_id,
                adset_id,
                creative_id,
                ad_id,
                ad_name,
                processed_at
            FROM launches_v2_processed
            WHERE webhook_status = 'failed'
              AND processed_at < NOW() - (%s * INTERVAL '1 minute')
            ORDER BY processed_at ASC
            LIMIT 100
            """,
            (retry_after_minutes,),
        )

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return [dict(row) for row in rows]

    except Exception as e:
        logging.error(f"Failed to get failed ads: {e}")
        return []


# -----------------------------
# Meta API
# -----------------------------
def get_ad_name_from_meta_api(ad_id: str) -> Optional[str]:
    """Fetch ad name directly from Meta API."""
    try:
        if not META_ACCESS_TOKEN:
            logging.warning("META_ACCESS_TOKEN not set, cannot fetch ad name from API")
            return None

        url = f"https://graph.facebook.com/v21.0/{ad_id}"
        params = {"fields": "name", "access_token": META_ACCESS_TOKEN}

        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()
            ad_name = data.get("name", "")
            logging.info(f"  Fetched ad name for {ad_id}: '{ad_name}'")
            return ad_name

        logging.warning(
            f"Failed to fetch ad name for {ad_id}: {response.status_code} - {response.text[:200]}"
        )
        return None

    except Exception as e:
        logging.warning(f"Error fetching ad name from Meta API: {e}")
        return None


# -----------------------------
# Processed table write (UPSERT)
# -----------------------------
def upsert_processed_rows(ads_with_names: List[Dict], status: str):
    """
    UPSERT into launches_v2_processed:
    - insert if missing
    - otherwise update existing row (status, processed_at, and ids/names)
    """
    if not ads_with_names:
        return

    try:
        import psycopg2

        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        for ad in ads_with_names:
            cursor.execute(
                """
                INSERT INTO launches_v2_processed
                    (launch_key, campaign_id, adset_id, creative_id, ad_id, ad_name, webhook_status, processed_at)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (launch_key) DO UPDATE SET
                    campaign_id    = EXCLUDED.campaign_id,
                    adset_id       = EXCLUDED.adset_id,
                    creative_id    = EXCLUDED.creative_id,
                    ad_id          = EXCLUDED.ad_id,
                    ad_name        = EXCLUDED.ad_name,
                    webhook_status = EXCLUDED.webhook_status,
                    processed_at   = NOW()
                """,
                (
                    ad["launch_key"],
                    ad.get("campaign_id"),
                    ad.get("adset_id"),
                    ad.get("creative_id"),
                    ad.get("ad_id"),
                    ad.get("ad_name"),
                    status,
                ),
            )

        conn.commit()
        cursor.close()
        conn.close()

        logging.info(f"✅ Upserted {len(ads_with_names)} row(s) to processed (status: {status})")

    except Exception as e:
        logging.error(f"Failed to upsert processed rows: {e}")


# -----------------------------
# Webhook Sender
# -----------------------------
def send_to_webhook(ads: List[Dict], retry: bool = False) -> Tuple[bool, List[Dict]]:
    """
    Send ads to Make.com webhook.

    Returns: (success, ads_with_names_for_db)

    We always try to fetch ad names and send clean names to Notion matching.
    """
    try:
        ads_data = []
        ads_with_names = []

        logging.info(f"Fetching ad names from Meta API for {len(ads)} ad(s)...")

        for ad in ads:
            ad_id = ad.get("ad_id")
            if not ad_id:
                logging.warning("⚠️  Skipping row with missing ad_id")
                continue

            ad_name_full = get_ad_name_from_meta_api(ad_id)
            if ad_name_full is None:
                logging.warning(f"⚠️  Skipping ad {ad_id} - couldn't fetch name")
                continue

            # Clean name for Notion matching
            ad_name_clean = ad_name_full.split(" //")[0].strip() if " //" in ad_name_full else ad_name_full

            ads_data.append(
                {
                    "ad_name": ad_name_clean,
                    "ad_id": ad.get("ad_id"),
                    "adset_id": ad.get("adset_id"),
                    "campaign_id": ad.get("campaign_id"),
                }
            )

            ads_with_names.append(
                {
                    "launch_key": ad.get("launch_key"),
                    "ad_name": ad_name_full,  # store full name in DB
                    "campaign_id": ad.get("campaign_id"),
                    "adset_id": ad.get("adset_id"),
                    "creative_id": ad.get("creative_id"),
                    "ad_id": ad.get("ad_id"),
                }
            )

        if not ads_data:
            logging.warning("❌ No ads to send (all failed to fetch names or missing ad_id)")
            return False, []

        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "count": len(ads_data),
            "ads": ads_data,
        }
        if retry:
            payload["retry"] = True

        logging.info(f"📤 Sending {len(ads_data)} ad(s) to webhook...")
        response = requests.post(
            WEBHOOK_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )

        if 200 <= response.status_code < 300:
            logging.info(f"✅ Successfully sent to webhook (status {response.status_code})")
            return True, ads_with_names

        logging.error(f"❌ Webhook failed with status {response.status_code}: {response.text[:200]}")
        return False, ads_with_names

    except Exception as e:
        logging.error(f"❌ Failed to send to webhook: {e}")
        return False, []


# -----------------------------
# Main Loop
# -----------------------------
def main():
    # Validate config
    if not DATABASE_URL:
        logging.error("❌ DATABASE_URL not set. Exiting.")
        sys.exit(1)

    if not WEBHOOK_URL:
        logging.error("❌ MAKE_WEBHOOK_URL not set. Exiting.")
        sys.exit(1)

    if not META_ACCESS_TOKEN:
        logging.error("❌ META_ACCESS_TOKEN not set. Exiting.")
        sys.exit(1)

    logging.info("=" * 70)
    logging.info("🚀 Meta Ads ID Notifier - Starting")
    logging.info("=" * 70)
    logging.info("📊 Database: Connected")
    logging.info(f"🔗 Webhook: {WEBHOOK_URL[:50]}...")
    logging.info(f"🔑 Meta Token: {'*' * 20}{META_ACCESS_TOKEN[-12:]}")
    logging.info(f"⏱️  Poll interval: {POLL_INTERVAL}s")
    logging.info(f"📦 Batch size: {BATCH_SIZE}")
    logging.info(f"🔄 Retry failed after: {RETRY_FAILED_AFTER_MINUTES} minutes")
    if RECENT_WINDOW_MINUTES_INT is not None:
        logging.info(f"🕒 Recent window: last {RECENT_WINDOW_MINUTES_INT} minute(s)")
    else:
        logging.info("🕒 Recent window: disabled (processes all unprocessed ads)")

    logging.info("=" * 70)

    init_processed_table()

    while True:
        try:
            # 1) Retry failures (optional)
            failed = get_failed_ads_to_retry(RETRY_FAILED_AFTER_MINUTES)
            if failed:
                logging.info(f"🔄 Found {len(failed)} failed ad(s) to retry")
                ok, ads_with_names = send_to_webhook(failed, retry=True)
                upsert_processed_rows(ads_with_names, "success" if ok else "failed")

            # 2) Poll launches_v2 (now with processed filtering)
            ads = get_ads_from_launches_v2(BATCH_SIZE)

            if not ads:
                logging.info("✨ No new ads to process")
                logging.info(f"💤 Sleeping {POLL_INTERVAL}s until next poll...")
                time.sleep(POLL_INTERVAL)
                continue

            logging.info(f"📋 Found {len(ads)} new ad(s) to process")
            ok, ads_with_names = send_to_webhook(ads, retry=False)
            upsert_processed_rows(ads_with_names, "success" if ok else "failed")

            logging.info(f"💤 Sleeping {POLL_INTERVAL}s until next poll...")
            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            logging.info("👋 Shutting down...")
            sys.exit(0)
        except Exception as e:
            logging.error(f"❌ Unexpected error in main loop: {e}")
            logging.info(f"💤 Sleeping {POLL_INTERVAL}s until next poll...")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
