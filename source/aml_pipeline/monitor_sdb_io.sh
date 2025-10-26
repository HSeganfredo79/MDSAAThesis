#!/bin/bash

# Output log file
LOG_FILE="iostat_sdb_$(date +%Y%m%d_%H%M%S).log"

# Device to monitor
DEVICE="sdb"

# Duration in seconds (12 hours = 43200 seconds)
DURATION=$((12 * 60 * 60))

# Interval between iostat snapshots (in seconds)
INTERVAL=15

# Total samples to collect
COUNT=$((DURATION / INTERVAL))

echo "Monitoring /dev/${DEVICE} with iostat every ${INTERVAL}s for 12 hours..."
echo "Logging to ${LOG_FILE}"
echo "Start time: $(date)" >> "$LOG_FILE"

# Header for CSV format
echo "Timestamp,rrqm/s,wrqm/s,r/s,w/s,rkB/s,wkB/s,avgrq-sz,avgqu-sz,await,r_await,w_await,svctm,%util" >> "$LOG_FILE"

# Collect data
for ((i=1; i<=COUNT; i++)); do
    TIMESTAMP=$(date +%Y-%m-%dT%H:%M:%S)
    iostat -dxk 1 2 | awk -v dev="$DEVICE" -v ts="$TIMESTAMP" '
        BEGIN { found = 0 }
        $1 == dev { 
            found = 1
            printf "%s,%s\n", ts, $0 
        }
        END {
            if (!found) {
                print ts ",DEVICE_NOT_FOUND"
            }
        }
    ' >> "$LOG_FILE"

    sleep "$INTERVAL"
done

echo "Monitoring completed at: $(date)" >> "$LOG_FILE"
