#!/bin/bash
# Prefect 3.x Deployment Registration Script with Cron Schedules
# make sure the work pool exists and your worker is running (it already is)
prefect work-pool ls

# deploy all entries defined in prefect.yaml
prefect deploy --all

# list deployed flows
prefect deployment ls
