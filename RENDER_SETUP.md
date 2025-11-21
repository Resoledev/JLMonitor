# Render Deployment Setup

## 🚨 Critical Issue: Data Persistence

Your current setup **loses all data on every restart** because Render's filesystem is ephemeral.

## ✅ Solution: Add Persistent Disk

### 1. In Render Dashboard
1. Go to your web service
2. Click **"Disks"** tab
3. Click **"Add Disk"**
4. Configure:
   - **Name:** `johnlewis-data`
   - **Mount Path:** `/data`
   - **Size:** 1 GB (free tier)

### 2. Update Your Code

Update file paths to use persistent disk:

```python
# backendv2.py - Change at top of file

import os

# Use persistent disk if on Render, otherwise local
if os.path.exists('/data'):
    # On Render with persistent disk
    PROJECT_DIR = '/data'
else:
    # Local development
    PROJECT_DIR = os.getcwd()

LOG_DIR = os.path.join(PROJECT_DIR, 'logs')
STATE_DIR = os.path.join(PROJECT_DIR, 'state')
CSV_FILE = os.path.join(PROJECT_DIR, 'johnlewisv2.csv')
PRICE_HISTORY_FILE = os.path.join(STATE_DIR, 'price_history.json')
```

### 3. Initialize Data on First Run

Add this to your `backendv2.py` main():

```python
def initialize_data():
    """Copy initial data to persistent disk if needed"""
    if not os.path.exists(CSV_FILE):
        # Copy from repo to persistent disk
        import shutil
        source_csv = os.path.join(os.path.dirname(__file__), 'johnlewisv2.csv')
        if os.path.exists(source_csv):
            shutil.copy(source_csv, CSV_FILE)
            print(f"Initialized CSV from repo to {CSV_FILE}")

# In main():
if __name__ == "__main__":
    initialize_data()
    main()
```

## 📋 Files to Commit to GitHub

```bash
git add backendv2.py          # Backend code
git add app.py                # Frontend code
git add static/               # CSS/JS
git add templates/            # HTML templates
git add requirements.txt      # Dependencies
git add johnlewisv2.csv       # INITIAL data snapshot
git add state/*.json          # INITIAL state (optional)
```

## 🔄 How It Works

1. **Code** comes from GitHub (updates on every deploy)
2. **Data** (CSV, state files) persists on Render Disk
3. Updates to data survive restarts ✅

## 💰 Cost

- **Persistent Disk:** FREE (1GB included on free tier)
- **Web Service:** FREE (750 hrs/month)

## 🚀 Alternative: Use PostgreSQL

For more robust solution:

1. Add PostgreSQL database (free tier available)
2. Store products in database instead of CSV
3. Much better for production use

Want help setting that up instead?
