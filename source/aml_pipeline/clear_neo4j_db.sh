#!/bin/bash
source .venv/bin/activate

echo "🧹 Neo4j Cleanup Utility"
echo "1 - Remove only :SENT relationships (preserve :Wallet nodes)"
echo "2 - FULL RESET: Delete all nodes and relationships"
read -p "Choose cleanup option (1 or 2): " choice

if [ "$choice" == "1" ]; then
    python3 cleanup_neo4j.py sent-only
elif [ "$choice" == "2" ]; then
    echo "⚠️  Are you sure you want to completely wipe Neo4j? This is IRREVERSIBLE."
    read -p "Type YES to confirm: " confirm
    if [ "$confirm" == "YES" ]; then
        python3 cleanup_neo4j.py full-reset
    else
        echo "❌ Operation cancelled."
    fi
else
    echo "❌ Invalid option."
fi
deactivate
