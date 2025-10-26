#!/bin/bash

echo "📦 Prefect Deployment Status:"
prefect deployment ls

echo -e "\n🟢 Prefect Service Status:"
prefect server services list-services

echo -e "\n📘 Recent Flow Runs:"
prefect flow-run ls --limit 10
