#!/bin/bash
# Job Finder — main pipeline
# Scheduled via launchd: 7 AM, 12 PM, 5 PM local time, Mon–Fri only

# Skip weekends (date +%u: 1=Mon ... 7=Sun)
DAY=$(date +%u)
if [ "$DAY" -ge 6 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') — Weekend, skipping" >> /tmp/job_finder.log
    exit 0
fi

export PATH="/Users/badrinathsanagavaram/anaconda3/bin:/usr/local/bin:/usr/bin:/bin"
cd /Users/badrinathsanagavaram/Documents/job-finder/backend

echo "$(date '+%Y-%m-%d %H:%M:%S') — pipeline starting" >> /tmp/job_finder.log
python3 main.py >> /tmp/job_finder.log 2>&1
echo "$(date '+%Y-%m-%d %H:%M:%S') — pipeline done" >> /tmp/job_finder.log
