#!/bin/bash
# Prefect 3.x Deployment Registration Script with Cron Schedules
for n in "Daily Enrichment/enrich-daily" "Daily Scoring/score-daily" "Weekly Training/train-weekly" "Drift Monitor/drift-monitor" "Kafka Producer/kafka-producer" "Kafka Consumer/kafka-consumer" "Kafka Streaming Loop/kafka-loop"; do prefect deployment delete "$n" || true; done
