# John Lewis Price Monitor

A price monitoring system that tracks John Lewis products, detects price changes, and sends Discord notifications for deals.

## Features

- 🔍 Scrapes John Lewis product categories for discounts
- 📊 Tracks price history over time
- 🔔 Discord webhook notifications for new deals and price drops
- 🎨 Modern web UI for browsing deals
- 📈 Recently added & recently reduced product tracking
- 🎯 Multi-variant product support

## Setup

### Requirements

-  Python 3.8+
- Dependencies listed in `requirements.txt`

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env and add your Discord webhook URL
```

### Running Locally

```bash
# Run the backend scraper
python backendv2.py

# Run the frontend (in separate terminal)
python app.py
```

Visit `http://localhost:5001` to view the web interface.

## Deployment to Render

This application is configured for free deployment on Render.

### Deploy Steps

1. Push this repository to GitHub
2. Create a new Web Service on Render
3. Connect your GitHub repository
4. Configure:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `python app.py`
   - **Environment Variables:** Add `DISCORD_WEBHOOK_URL`

### Auto-Update Script

After scraping completes, run the auto-commit script to push updates:

```bash
./auto_commit.sh
```

This commits and pushes the updated CSV and state files to GitHub, which triggers a Render deployment.

## File Structure

```
JohnLewisMonitor/
├── backendv2.py          # Price scraper (runs 1x/day)
├── app.py                # Flask web frontend
├── auto_commit.sh        # Auto-commit script
├── johnlewisv2.csv       # Product database
├── state/                # Price history & state
│   ├── price_history.json
│   ├── category_state.json
│   └── boots_state.json
├── static/               # CSS & JavaScript
├── templates/            # HTML templates
└── logs/                 # Application logs
```

## Configuration

Edit these constants in `backendv2.py`:

- `CATEGORY_URLS`: Categories to monitor
- `NOTIFY_EVERY_CYCLES`: Notification frequency
- `RECENTLY_ADDED_HOURS`: Threshold for "recently added" badge
- `DAYS_TO_KEEP_UNSEEN`: Days to keep products in database

## License

MIT
