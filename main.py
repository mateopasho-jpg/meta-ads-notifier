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

# -----------------------------
# Database Setup
# -----------------------------

def init_processed_table():
    """Create the processed table if it doesn't exist."""
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Create processed table (same structure as launches_v2)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS launches_v2_processed (
                launch_key TEXT PRIMARY KEY,
                payload_sha256 TEXT,
                created_at TIMESTAMPTZ,
                processed_at TIMESTAMPTZ DEFAULT NOW(),
                campaign_id TEXT,
                adset_id TEXT,
                creative_id TEXT,
                ad_id TEXT,
                campaign_name TEXT,
                adset_name TEXT,
                product TEXT,
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
        cursor.execute("""
            SELECT 
                l.launch_key,
                l.campaign_id,
                l.adset_id,
                l.creative_id,
                l.ad_id,
                l.adset_name,
                l.campaign_name,
                l.product
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


def mark_as_processed(ads_with_names: List[Dict], status: str = 'success'):
    """Move ads from launches_v2 to launches_v2_processed.
    
    Args:
        ads_with_names: List of dicts with 'launch_key' and 'ad_name'
        status: 'success' or 'failed'
    """
    if not ads_with_names:
        return
        
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Copy to processed table with ad_name
        for ad in ads_with_names:
            cursor.execute("""
                INSERT INTO launches_v2_processed 
                    (launch_key, payload_sha256, created_at, campaign_id, adset_id, 
                     creative_id, ad_id, campaign_name, adset_name, product, ad_name, webhook_status)
                SELECT 
                    launch_key, payload_sha256, created_at, campaign_id, adset_id,
                    creative_id, ad_id, campaign_name, adset_name, product, %s, %s
                FROM launches_v2
                WHERE launch_key = %s
            """, (ad['ad_name'], status, ad['launch_key']))
        
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
                "ad_name": "3815_0_Rosa Glanz",
                "ad_id": "120239779109310430",
                "adset_id": "120239779108750430",
                "campaign_id": "120236472829790430",
                "product": "rosa"
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
            ad_name = get_ad_name_from_meta_api(ad['ad_id'])
            
            # If API call failed, skip this ad for now
            if ad_name is None:
                logging.warning(f"⚠️  Skipping ad {ad['ad_id']} - couldn't fetch name")
                continue
            
            ad_info = {
                "ad_name": ad_name,
                "ad_id": ad['ad_id'],
                "adset_id": ad['adset_id'],
                "campaign_id": ad['campaign_id'],
                "product": ad.get('product', ''),
            }
            
            ads_data.append(ad_info)
            ads_with_names.append({
                'launch_key': ad['launch_key'],
                'ad_name': ad_name
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
    logging.info("=" * 70)
    
    # Initialize processed table
    init_processed_table()
    
    # Main loop
    while True:
        try:
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
