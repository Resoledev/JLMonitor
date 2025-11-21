#!/bin/bash
# Auto-commit and push updates after scraping

# Exit on error
set -e

echo "🔄 Committing updates to GitHub..."

cd "$(dirname "$0")"

# Add updated files
git add johnlewisv2.csv
git add state/*.json
git add logs/*.log 2>/dev/null || true  # Add logs if they exist

# Check if there are changes
if git diff-index --quiet HEAD --; then
    echo "✅ No changes to commit"
    exit 0
fi

# Commit with timestamp
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
git commit -m "Auto-update: $TIMESTAMP"

# Push to GitHub
git push origin main

echo "✅ Changes pushed to GitHub successfully!"
