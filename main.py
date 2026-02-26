#!/usr/bin/env python3
"""
Meta Ads ID Notifier - Standalone Service
==========================================

Standalone service that:
1. Reads new entries from launches_v2 table
2. Extracts ad name, ad_id, adset_id, campaign_id
3. Sends to Make.com webhook
4. Moves processed entries to launches_v2_processed table

Deploy as independent Railway service.
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone
from typing import List, Dict
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    stream=sys.stdout
)

# -----------------------------
# Configuration
# -----------------------------

DATABASE_URL = os.getenv('DATABASE_URL')  # PostgreSQL connection
WEBHOOK_URL = os.getenv('MAKE_WEBHOOK_URL')  # Make.com webhook
META_ACCESS_TOKEN = os.getenv('META_ACCESS_TOKEN')  # Meta API token
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL_SECONDS', '60'))  # 60 seconds
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))  # Process 100 at a time
RETRY_FAILED_AFTER_MINUTES = int(os.getenv('RETRY_FAILED_AFTER_MINUTES', '5'))  # Retry failed ads after 5 minutes

# -----------------------------
# Database Setup
# -----------------------------

def init_processed_table():
    """Create the processed table if it doesn't exist."""
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Create processed table with only the columns we actually use
        cursor.execute("""
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
        """)
        
        # Create index for faster lookups
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_at 
            ON launches_v2_processed(processed_at DESC)
        """)
        
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

def get_unprocessed_ads(limit: int = 100) -> List[Dict]:
    """Get unprocessed ads from launches_v2.
    
    Returns ads that are NOT in launches_v2_processed yet.
    """
    try:
        import psycopg2
        import psycopg2.extras
        
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get ads from launches_v2 that aren't in processed table
        # Only select columns that definitely exist
        cursor.execute("""
            SELECT 
                l.launch_key,
                l.campaign_id,
                l.adset_id,
                l.creative_id,
                l.ad_id
            FROM launches_v2 l
            LEFT JOIN launches_v2_processed p ON l.launch_key = p.launch_key
            WHERE p.launch_key IS NULL
            ORDER BY l.created_at ASC
            LIMIT %s
        """, (limit,))
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return [dict(row) for row in rows]
        
    except Exception as e:
        logging.error(f"Failed to get unprocessed ads: {e}")
        return []


def get_ad_name_from_meta_api(ad_id: str) -> str:
    """Fetch ad name directly from Meta API.
    
    The ad name format should be: 3815_0_Rosa Glanz
    """
    try:
        if not META_ACCESS_TOKEN:
            logging.warning("META_ACCESS_TOKEN not set, cannot fetch ad name from API")
            return None
        
        url = f"https://graph.facebook.com/v21.0/{ad_id}"
        params = {
            'fields': 'name',
            'access_token': META_ACCESS_TOKEN
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            ad_name = data.get('name', '')
            logging.info(f"  Fetched ad name for {ad_id}: '{ad_name}'")
            return ad_name
        else:
            logging.warning(f"Failed to fetch ad name for {ad_id}: {response.status_code} - {response.text[:200]}")
            return None
            
    except Exception as e:
        logging.warning(f"Error fetching ad name from Meta API: {e}")
        return None


def get_failed_ads_to_retry(retry_after_minutes: int) -> List[Dict]:
    """Get failed ads that are old enough to retry.
    
    Returns ads that failed more than retry_after_minutes ago.
    """
    try:
        import psycopg2
        import psycopg2.extras
        from datetime import timedelta
        
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get failed ads that are older than retry threshold
        cursor.execute("""
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
            AND processed_at < NOW() - INTERVAL '%s minutes'
            ORDER BY processed_at ASC
            LIMIT 100
        """, (retry_after_minutes,))
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return [dict(row) for row in rows]
        
    except Exception as e:
        logging.error(f"Failed to get failed ads: {e}")
        return []


def retry_failed_ads(ads: List[Dict]) -> bool:
    """Retry sending failed ads to webhook.
    
    Returns True if successful, False otherwise.
    """
    try:
        if not ads:
            return False
        
        # Prepare payload with cleaned ad names
        ads_data = []
        for ad in ads:
            # Clean the ad name (remove " // Video // Mehr dazu // LPXXX" part)
            ad_name_full = ad['ad_name']
            ad_name_clean = ad_name_full.split(" //")[0].strip() if " //" in ad_name_full else ad_name_full
            
            ads_data.append({
                "ad_name": ad_name_clean,  # Send clean name for Notion matching
                "ad_id": ad['ad_id'],
                "adset_id": ad['adset_id'],
                "campaign_id": ad['campaign_id'],
            })
        
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "count": len(ads_data),
            "ads": ads_data,
            "retry": True  # Flag to indicate this is a retry
        }
        
        logging.info(f"🔄 Retrying {len(ads_data)} failed ad(s)...")
        
        response = requests.post(
            WEBHOOK_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if 200 <= response.status_code < 300:
            logging.info(f"✅ Retry successful (status {response.status_code})")
            
            # Update status to success
            try:
                import psycopg2
                conn = psycopg2.connect(DATABASE_URL)
                cursor = conn.cursor()
                
                launch_keys = [ad['launch_key'] for ad in ads]
                cursor.execute("""
                    UPDATE launches_v2_processed
                    SET webhook_status = 'success',
                        processed_at = NOW()
                    WHERE launch_key = ANY(%s)
                """, (launch_keys,))
                
                conn.commit()
                cursor.close()
                conn.close()
                
                logging.info(f"✅ Updated {len(ads)} ad(s) to success status")
                
            except Exception as e:
                logging.error(f"Failed to update retry status: {e}")
            
            return True
        else:
            logging.error(f"❌ Retry failed with status {response.status_code}: {response.text[:200]}")
            return False
            
    except Exception as e:
        logging.error(f"❌ Failed to retry: {e}")
        return False


def mark_as_processed(ads_with_names: List[Dict], status: str = 'success'):
    """Move ads from launches_v2 to launches_v2_processed.
    
    Args:
        ads_with_names: List of dicts with 'launch_key', 'ad_name', 'campaign_id', 'adset_id', 'creative_id', 'ad_id'
        status: 'success' or 'failed'
    """
    if not ads_with_names:
        return
        
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Insert into processed table
        for ad in ads_with_names:
            cursor.execute("""
                INSERT INTO launches_v2_processed 
                    (launch_key, campaign_id, adset_id, creative_id, ad_id, ad_name, webhook_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (launch_key) DO NOTHING
            """, (
                ad['launch_key'],
                ad.get('campaign_id'),
                ad.get('adset_id'),
                ad.get('creative_id'),
                ad.get('ad_id'),
                ad['ad_name'],
                status
            ))
        
        # Delete from original table
        launch_keys = [ad['launch_key'] for ad in ads_with_names]
        cursor.execute("""
            DELETE FROM launches_v2
            WHERE launch_key = ANY(%s)
        """, (launch_keys,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"✅ Marked {len(ads_with_names)} ad(s) as processed (status: {status})")
        
    except Exception as e:
        logging.error(f"Failed to mark ads as processed: {e}")


# -----------------------------
# Webhook Sender
# -----------------------------

def send_to_webhook(ads: List[Dict]) -> tuple[bool, List[Dict]]:
    """Send ads to Make.com webhook.
    
    Returns: (success, ads_with_names)
    
    Payload format:
    {
        "timestamp": "2026-02-25T16:00:00Z",
        "count": 4,
        "ads": [
            {
                "ad_name": "3830_0_Rosa Glanz",  // Clean name for Notion matching
                "ad_id": "120239779109310430",
                "adset_id": "120239779108750430",
                "campaign_id": "120236472829790430"
            },
            ...
        ]
    }
    """
    try:
        # Prepare payload
        ads_data = []
        ads_with_names = []
        
        logging.info(f"Fetching ad names from Meta API for {len(ads)} ad(s)...")
        
        for ad in ads:
            # Try to get ad name from Meta API
            ad_name_full = get_ad_name_from_meta_api(ad['ad_id'])
            
            # If API call failed, skip this ad for now
            if ad_name_full is None:
                logging.warning(f"⚠️  Skipping ad {ad['ad_id']} - couldn't fetch name")
                continue
            
            # Extract clean name (before first " //")
            # Example: "3830_0_Rosa Glanz // Video // Mehr dazu // LP260" -> "3830_0_Rosa Glanz"
            ad_name_clean = ad_name_full.split(" //")[0].strip() if " //" in ad_name_full else ad_name_full
            
            ad_info = {
                "ad_name": ad_name_clean,  # Clean name for Notion matching
                "ad_id": ad['ad_id'],
                "adset_id": ad['adset_id'],
                "campaign_id": ad['campaign_id'],
            }
            
            ads_data.append(ad_info)
            ads_with_names.append({
                'launch_key': ad['launch_key'],
                'ad_name': ad_name_full,  # Store full name in database
                'campaign_id': ad['campaign_id'],
                'adset_id': ad['adset_id'],
                'creative_id': ad['creative_id'],
                'ad_id': ad['ad_id'],
            })
        
        if not ads_data:
            logging.warning("❌ No ads to send (all failed to fetch names)")
            return False, []
        
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "count": len(ads_data),
            "ads": ads_data
        }
        
        logging.info(f"📤 Sending {len(ads_data)} ad(s) to webhook...")
        
        response = requests.post(
            WEBHOOK_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if 200 <= response.status_code < 300:
            logging.info(f"✅ Successfully sent to webhook (status {response.status_code})")
            return True, ads_with_names
        else:
            logging.error(f"❌ Webhook failed with status {response.status_code}: {response.text[:200]}")
            return False, ads_with_names
            
    except Exception as e:
        logging.error(f"❌ Failed to send to webhook: {e}")
        return False, []


# -----------------------------
# Main Loop
# -----------------------------

def main():
    """Main service loop."""
    
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
    logging.info(f"📊 Database: Connected")
    logging.info(f"🔗 Webhook: {WEBHOOK_URL[:50]}...")
    logging.info(f"🔑 Meta Token: {'*' * 20}{META_ACCESS_TOKEN[-10:]}")
    logging.info(f"⏱️  Poll interval: {POLL_INTERVAL}s")
    logging.info(f"📦 Batch size: {BATCH_SIZE}")
    logging.info(f"🔄 Retry failed after: {RETRY_FAILED_AFTER_MINUTES} minutes")
    logging.info("=" * 70)
    
    # Initialize processed table
    init_processed_table()
    
    # Track cycles for retry logic (retry every 5 cycles = 5 minutes with 60s poll)
    cycle_count = 0
    
    # Main loop
    while True:
        try:
            cycle_count += 1
            
            # Every 5 minutes (5 cycles), check for failed ads to retry
            if cycle_count % 5 == 0:
                failed_ads = get_failed_ads_to_retry(RETRY_FAILED_AFTER_MINUTES)
                if failed_ads:
                    logging.info(f"\n🔄 Found {len(failed_ads)} failed ad(s) to retry")
                    retry_failed_ads(failed_ads)
            
            # Get unprocessed ads
            ads = get_unprocessed_ads(limit=BATCH_SIZE)
            
            if ads:
                logging.info(f"\n🔍 Found {len(ads)} unprocessed ad(s)")
                
                # Send to webhook (also fetches ad names)
                success, ads_with_names = send_to_webhook(ads)
                
                # Mark as processed
                if ads_with_names:
                    status = 'success' if success else 'failed'
                    mark_as_processed(ads_with_names, status)
                else:
                    logging.warning("⚠️  No ads were successfully processed this cycle")
                
            else:
                logging.info("✨ No new ads to process")
            
        except KeyboardInterrupt:
            logging.info("\n👋 Shutting down gracefully...")
            break
            
        except Exception as e:
            logging.error(f"❌ Error in main loop: {e}", exc_info=True)
        
        # Sleep until next poll
        logging.info(f"💤 Sleeping {POLL_INTERVAL}s until next poll...\n")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
