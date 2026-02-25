# Meta Ads ID Notifier 📢

Standalone service that monitors the `launches_v2` database table and sends ad creation data to Make.com for Notion updates.

## 🎯 What It Does

1. **Polls** the `launches_v2` table every 60 seconds
2. **Fetches** ad name from Meta API (format: `3815_0_Rosa Glanz`)
3. **Sends** to Make.com webhook:
   - `ad_name` (e.g., "3815_0_Rosa Glanz")
   - `ad_id`
   - `adset_id`
   - `campaign_id`
   - `product`
4. **Moves** processed entries to `launches_v2_processed` table

## 🏗️ Architecture

```
┌──────────────────────┐
│   launches_v2        │ ← New ads written here by worker
│   (PostgreSQL)       │
└──────────────────────┘
         │
         │ (reads unprocessed)
         ▼
┌──────────────────────┐
│   This Service       │
│   - Reads new ads    │
│   - Calls Meta API   │
│   - Sends webhook    │
│   - Archives         │
└──────────────────────┘
         │
         ├────► Meta Graph API (GET /ad_id?fields=name)
         │           │
         │           └──► Returns: "3815_0_Rosa Glanz"
         │
         └────► Make.com Webhook
                     │
                     └──► Notion Update (match by ad_name)

┌──────────────────────┐
│ launches_v2_processed│ ← Archive of processed ads
│ (PostgreSQL)         │
└──────────────────────┘
```

## 📦 Files

```
notifier_standalone/
├── main.py              # Main service script
├── requirements.txt     # Python dependencies
├── README.md           # This file
├── .env.example        # Environment variable template
├── .gitignore          # Git ignore rules
└── railway.json        # Railway deployment config
```

## 🚀 Quick Start

### 1. Environment Variables

Create a `.env` file or set in Railway:

```bash
# Required
DATABASE_URL=postgresql://user:pass@host:5432/dbname
MAKE_WEBHOOK_URL=https://hook.eu1.make.com/xxxxxxxxxxxxx
META_ACCESS_TOKEN=EAAxxxxxxxxxxxxxxxxxxxxxxxxxx

# Optional
POLL_INTERVAL_SECONDS=60
BATCH_SIZE=100
```

### 2. Deploy to Railway

#### Option A: GitHub Deploy (Recommended)

1. Create new GitHub repo
2. Push these files
3. In Railway:
   - Click "New Project"
   - Select "Deploy from GitHub repo"
   - Choose your repo
   - Add environment variables
   - Deploy ✅

#### Option B: Railway CLI

```bash
railway login
railway init
railway link
railway up
```

### 3. Make.com Webhook Setup

Create a Make.com scenario:

**Step 1: Add Webhook Module**
- Module: Webhooks → Custom Webhook
- Copy the webhook URL
- Use it as `MAKE_WEBHOOK_URL`

**Step 2: Add Iterator**
- Array: `{{ads}}`

**Step 3: Add Notion Module**
- Action: Update a database item
- Search: Where "Ad Name" contains `{{ad_name}}`
- Update:
  - Ad ID: `{{ad_id}}`
  - AdSet ID: `{{adset_id}}`
  - Campaign ID: `{{campaign_id}}`
  - Status: ✅ Created

## 📨 Webhook Payload

The service sends this JSON to Make.com:

```json
{
  "timestamp": "2026-02-25T16:00:00.123456+00:00",
  "count": 4,
  "ads": [
    {
      "ad_name": "3815_0_Rosa Glanz",
      "ad_id": "120239779109310430",
      "adset_id": "120239779108750430",
      "campaign_id": "120236472829790430",
      "product": "rosa"
    },
    {
      "ad_name": "3830_0_Rosa Glanz",
      "ad_id": "120239779109670430",
      "adset_id": "120239779108750430",
      "campaign_id": "120236472829790430",
      "product": "rosa"
    }
  ]
}
```

## 🔄 How It Works

### Processing Flow

```mermaid
graph TD
    A[Service starts] --> B[Create launches_v2_processed table]
    B --> C[Every 60 seconds]
    C --> D{New ads in launches_v2?}
    D -->|No| C
    D -->|Yes| E[For each ad: Call Meta API]
    E --> F[Get ad name: 3815_0_Rosa Glanz]
    F --> G[Build webhook payload]
    G --> H[Send to Make.com]
    H --> I[Move to launches_v2_processed]
    I --> J[Delete from launches_v2]
    J --> C
```

### Database Tables

#### `launches_v2` (existing)
- Worker writes new ads here
- Service reads from here
- Small table (only unprocessed ads)

#### `launches_v2_processed` (created automatically)
- Archive of all processed ads
- Includes `ad_name`, `webhook_status`, `processed_at`
- Prevents re-processing
- Keeps audit trail

### Why Move to Processed Table?

✅ **Fast queries** - Small `launches_v2` table = instant lookups  
✅ **No duplicates** - LEFT JOIN ensures each ad processed exactly once  
✅ **Clean separation** - Unprocessed vs processed  
✅ **Audit trail** - All processed ads archived with status  
✅ **Easy debugging** - Can see what was sent and when  

## 📊 Monitoring

### Check Unprocessed Count

```sql
SELECT COUNT(*) as unprocessed_ads 
FROM launches_v2;
```

### Check Processing Stats

```sql
SELECT 
    webhook_status,
    COUNT(*) as count,
    MAX(processed_at) as last_processed
FROM launches_v2_processed
GROUP BY webhook_status;
```

Should show:
```
 webhook_status | count | last_processed
----------------+-------+-------------------
 success        |   432 | 2026-02-25 16:05:00
 failed         |     2 | 2026-02-25 15:30:00
```

### Recent Processed Ads

```sql
SELECT 
    ad_name,
    ad_id, 
    campaign_id, 
    webhook_status, 
    processed_at
FROM launches_v2_processed
ORDER BY processed_at DESC
LIMIT 20;
```

### Failed Webhooks

```sql
SELECT * 
FROM launches_v2_processed
WHERE webhook_status = 'failed'
ORDER BY processed_at DESC;
```

## 🐛 Troubleshooting

### No ads being processed?

**Check if ads exist:**
```sql
SELECT * FROM launches_v2 LIMIT 5;
```

**Check service logs:**
```
railway logs
```

Look for: `Found X unprocessed ad(s)`

### Webhook failing?

- ✅ Verify `MAKE_WEBHOOK_URL` is correct
- ✅ Check Make.com scenario is **active** (green toggle)
- ✅ Test webhook with webhook.site first
- ✅ Check service logs for error details

### Can't fetch ad names from Meta?

**Common issues:**

1. **Token not set**: Check `META_ACCESS_TOKEN` env var
2. **Token expired**: Generate new token in Meta Business Settings
3. **Wrong permissions**: Token needs `ads_read` permission
4. **Ad doesn't exist yet**: Wait a few seconds after creation

**Test your token manually:**
```bash
curl "https://graph.facebook.com/v21.0/YOUR_AD_ID?fields=name&access_token=YOUR_TOKEN"
```

Should return:
```json
{
  "name": "3815_0_Rosa Glanz",
  "id": "120239779109310430"
}
```

### Want to reprocess ads?

Move specific ads back from processed to main table:

```sql
-- Move one ad back
INSERT INTO launches_v2 
SELECT 
    launch_key, payload_sha256, created_at, campaign_id, adset_id, 
    creative_id, ad_id, campaign_name, adset_name, product
FROM launches_v2_processed
WHERE ad_id = '120239779109310430';

-- Delete from processed
DELETE FROM launches_v2_processed 
WHERE ad_id = '120239779109310430';
```

### Service keeps restarting?

Check Railway logs for startup errors:

```bash
railway logs --follow
```

Common issues:
- Missing environment variables
- Database connection failed
- Invalid Meta token

## ⚙️ Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | ✅ Yes | - | PostgreSQL connection string |
| `MAKE_WEBHOOK_URL` | ✅ Yes | - | Make.com webhook URL |
| `META_ACCESS_TOKEN` | ✅ Yes | - | Meta API access token |
| `POLL_INTERVAL_SECONDS` | No | 60 | Seconds between checks |
| `BATCH_SIZE` | No | 100 | Max ads per cycle |

## 🧪 Local Development

### Setup

```bash
# Clone repo
git clone <your-repo>
cd notifier_standalone

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env

# Edit .env with your credentials
nano .env
```

### Run

```bash
python main.py
```

### Test with webhook.site

Instead of Make.com, use webhook.site for testing:

1. Go to https://webhook.site
2. Copy your unique URL
3. Set `MAKE_WEBHOOK_URL=https://webhook.site/your-unique-id`
4. Run service
5. See payloads appear in webhook.site

## 📝 Logs

### Normal Operation

```
2026-02-25 16:00:00 INFO 🚀 Meta Ads ID Notifier - Starting
2026-02-25 16:00:00 INFO 📊 Database: Connected
2026-02-25 16:00:00 INFO 🔗 Webhook: https://hook.eu1.make.com/...
2026-02-25 16:00:00 INFO ✅ Processed table initialized
2026-02-25 16:01:00 INFO 🔍 Found 4 unprocessed ad(s)
2026-02-25 16:01:00 INFO Fetching ad names from Meta API for 4 ad(s)...
2026-02-25 16:01:01 INFO   Fetched ad name for 120239779109310430: '3815_0_Rosa Glanz'
2026-02-25 16:01:02 INFO   Fetched ad name for 120239779109670430: '3830_0_Rosa Glanz'
2026-02-25 16:01:03 INFO   Fetched ad name for 120239779110230430: '3859_0_Rosa Glanz'
2026-02-25 16:01:04 INFO   Fetched ad name for 120239779110610430: '3861_0_Rosa Glanz'
2026-02-25 16:01:04 INFO 📤 Sending 4 ad(s) to webhook...
2026-02-25 16:01:05 INFO ✅ Successfully sent to webhook (status 200)
2026-02-25 16:01:05 INFO ✅ Marked 4 ad(s) as processed (status: success)
2026-02-25 16:01:05 INFO 💤 Sleeping 60s until next poll...
```

### Error Handling

If Meta API fails:
```
2026-02-25 16:01:02 WARNING ⚠️  Skipping ad 120239779109310430 - couldn't fetch name
```

If webhook fails:
```
2026-02-25 16:01:05 ERROR ❌ Webhook failed with status 500: Internal Server Error
2026-02-25 16:01:05 INFO ✅ Marked 4 ad(s) as processed (status: failed)
```

## 🔒 Security

- Never commit `.env` file
- Keep `META_ACCESS_TOKEN` secret
- Rotate tokens if exposed
- Use Railway's secret variables

## 📄 License

MIT

## 🤝 Support

Issues? Check:
1. Railway logs
2. Database table exists
3. Meta token is valid
4. Make.com webhook is active

Still stuck? Review the troubleshooting section above.
