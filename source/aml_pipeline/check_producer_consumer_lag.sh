#!/bin/bash

GROUP="aml_pipeline_consumer"
TOPIC="usdc-transactions"
BROKER="192.168.0.4:9092"
LAG_ALERT_THRESHOLD=1000

echo ""
echo "🔍 Checking offsets for:"
echo "   • Group : $GROUP"
echo "   • Topic : $TOPIC"
echo "   • Broker: $BROKER"
echo "----------------------------------------------------------------------------------------------------------------------------------"
echo "GROUP                      TOPIC             PARTITION  CURRENT-OFFSET     LOG-END-OFFSET     LAG        CLIENT-ID            HOST"
echo "----------------------------------------------------------------------------------------------------------------------------------"

kafka-consumer-groups.sh --bootstrap-server "$BROKER" --describe --group "$GROUP" | grep "$TOPIC" | while read -r line; do
    # Split fields using awk
    group=$(echo "$line" | awk '{print $1}')
    topic=$(echo "$line" | awk '{print $2}')
    partition=$(echo "$line" | awk '{print $3}')
    current_offset=$(echo "$line" | awk '{print $4}')
    log_end_offset=$(echo "$line" | awk '{print $5}')
    lag=$(echo "$line" | awk '{print $6}')
    client_id=$(echo "$line" | awk '{print $7}')
    host=$(echo "$line" | awk '{print $8}')

    # Output formatted row
    printf "%-25s %-17s %-10s %-18s %-18s %-10s %-20s %s\n" \
        "$group" "$topic" "$partition" "$current_offset" "$log_end_offset" "$lag" "$client_id" "$host"

    # Check if lag is a valid integer
    if [[ "$lag" =~ ^[0-9]+$ ]]; then
        if [ "$lag" -gt "$LAG_ALERT_THRESHOLD" ]; then
            echo "  🚨 Lag exceeds ${LAG_ALERT_THRESHOLD}!"
        fi
    else
        echo "  ⚠️ Warning: Invalid lag value detected ('$lag')"
    fi
done

echo "----------------------------------------------------------------------------------------------------------------------------------"
echo "✅ Offset check complete."
