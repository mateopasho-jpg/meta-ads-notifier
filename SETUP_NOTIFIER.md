# Setup Guide - Step by Step

Follow these steps to deploy the Meta Ads ID Notifier.

## Prerequisites

- [ ] PostgreSQL database with `launches_v2` table
- [ ] Make.com account
- [ ] Meta access token with `ads_read` permission
- [ ] Railway account (or any hosting platform)

## Step 1: Create GitHub Repository

1. Create a new GitHub repository (e.g., `meta-ads-notifier`)
2. Clone it locally:
   ```bash
   git clone https://github.com/your-username/meta-ads-notifier.git
   cd meta-ads-notifier
   ```
3. Copy all files from `notifier_standalone/` into the repo root
4. Commit and push:
   ```bash
   git add .
   git commit -m "Initial commit"
   git push origin main
   ```

## Step 2: Create Make.com Webhook

1. Go to [Make.com](https://make.com)
2. Click **"Create a new scenario"**
3. Add **Webhooks** module → **Custom Webhook**
4. Click **"Create a webhook"**
5. Give it a name: "Meta Ads Notifier"
6. **Copy the webhook URL** (looks like: `https://hook.eu1.make.com/xxxxx`)
7. Keep this tab open - we'll build the scenario later

## Step 3: Get Meta Access Token

### Option A: Use Your Existing Token

If you already have a Meta access token from your worker, use that.

### Option B: Generate New Token

1. Go to [Meta Business Settings](https://business.facebook.com/settings)
2. Navigate to **System Users**
3. Select your system user
4. Click **Generate New Token**
5. Select your ad account
6. Check permissions: **ads_read**
7. Generate and **copy the token**

## Step 4: Deploy to Railway

1. Go to [Railway](https://railway.app)
2. Click **"New Project"**
3. Select **"Deploy from GitHub repo"**
4. Choose your repository
5. Railway will auto-detect it's a Python project

### Set Environment Variables

In Railway, go to your service → **Variables** tab:

```
DATABASE_URL = postgresql://user:pass@host:5432/dbname
MAKE_WEBHOOK_URL = https://hook.eu1.make.com/xxxxx
META_ACCESS_TOKEN = EAAxxxxxxxxxxxxxxxxxxxx
POLL_INTERVAL_SECONDS = 60
BATCH_SIZE = 100
```

**Important:** 
- Copy `DATABASE_URL` from your existing worker service (if it's in the same Railway project, use the reference: `${{Postgres.DATABASE_URL}}`)
- Use the webhook URL from Step 2
- Use the token from Step 3

### Deploy

Click **"Deploy"** and watch the logs.

You should see:
```
🚀 Meta Ads ID Notifier - Starting
📊 Database: Connected
✅ Processed table initialized
💤 Sleeping 60s until next poll...
```

## Step 5: Build Make.com Scenario

Go back to Make.com:

### Module 1: Webhook (Already Added)
Your webhook from Step 2

### Module 2: Iterator
- Click the **+** button after webhook
- Search for "Iterator"
- **Array**: Click and select `ads` from the webhook output

### Module 3: Notion - Update Database Item

- Click the **+** button after iterator
- Search for "Notion"
- Choose **Update a database item**
- **Connect your Notion account** (if not already)
- **Select your database**
- **Search for the item**: 
  - Field: "Ad Name" (or whatever field you store ad names)
  - Condition: "contains"
  - Value: `{{ad_name}}` (from iterator)

- **Properties to Update**:
  - **Ad ID**: `{{ad_id}}`
  - **AdSet ID**: `{{adset_id}}`
  - **Campaign ID**: `{{campaign_id}}`
  - **Status**: Select "Created" or "✅ Live" (however you track status)

**Save the scenario** and **turn it ON** (toggle should be green).

## Step 6: Test End-to-End

### Trigger a Test

1. Wait for your worker to create new ads (or create them manually)
2. The notifier will pick them up within 60 seconds
3. Check Railway logs:
   ```
   🔍 Found 4 unprocessed ad(s)
   Fetching ad names from Meta API...
   📤 Sending 4 ad(s) to webhook...
   ✅ Successfully sent to webhook (status 200)
   ```

4. Check Make.com scenario - you should see it executed
5. Check Notion - the rows should be updated with IDs

### Verify in Database

```sql
-- Check processed ads
SELECT * FROM launches_v2_processed ORDER BY processed_at DESC LIMIT 10;

-- Check unprocessed (should be empty or very few)
SELECT COUNT(*) FROM launches_v2;
```

## Step 7: Monitor

### Railway Logs

```bash
railway logs --follow
```

Or in Railway dashboard → Service → Logs tab

### Database Health

```sql
-- Processing stats
SELECT 
    webhook_status,
    COUNT(*) as total,
    MAX(processed_at) as last_processed
FROM launches_v2_processed
GROUP BY webhook_status;
```

Should show mostly "success" entries.

## Troubleshooting

### Service won't start

**Check Railway logs for:**
- `DATABASE_URL not set` → Add the env var
- `MAKE_WEBHOOK_URL not set` → Add the env var
- `META_ACCESS_TOKEN not set` → Add the env var
- `Failed to initialize processed table` → Check database connection

### No ads being processed

**Check:**
1. Are there ads in `launches_v2`?
   ```sql
   SELECT * FROM launches_v2 LIMIT 5;
   ```
2. Is the worker creating ads?
3. Are ads already in `launches_v2_processed`?

### Webhook not receiving data

**Check:**
1. Is Make.com scenario **active** (green toggle)?
2. Test with webhook.site:
   - Temporarily set `MAKE_WEBHOOK_URL=https://webhook.site/your-id`
   - Restart Railway service
   - See if payload appears in webhook.site
3. Check Make.com scenario execution history

### Can't fetch ad names

**Check Railway logs for:**
- `Failed to fetch ad name` → Token might be invalid
- Test token manually:
  ```bash
  curl "https://graph.facebook.com/v21.0/AD_ID?fields=name&access_token=YOUR_TOKEN"
  ```

### Ads processed but Notion not updated

**Check:**
1. Make.com scenario executed? (Check execution history)
2. Search query in Notion module finding the right page?
3. Ad name format matches what's in Notion?
4. Field names are correct?

## Success Checklist

- [ ] Service deployed to Railway
- [ ] All environment variables set
- [ ] Service logs show successful startup
- [ ] `launches_v2_processed` table created
- [ ] Make.com scenario active
- [ ] Test ad created and processed
- [ ] Notion updated with IDs
- [ ] Monitoring set up

## Next Steps

Once everything is working:

1. **Set up alerts**: Use Railway's monitoring to alert on failures
2. **Backup data**: Regularly backup `launches_v2_processed` table
3. **Monitor costs**: Check Railway and Make.com usage
4. **Scale if needed**: Increase `BATCH_SIZE` if processing many ads

## Support

If you get stuck:
1. Check Railway logs first
2. Verify all environment variables
3. Test each component separately (database, Meta API, webhook)
4. Check Make.com execution logs
