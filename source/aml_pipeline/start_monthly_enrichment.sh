#!/bin/bash
# Usage: ./run_monthly_enrichment.sh 202504

if [ $# -ne 1 ]; then
  echo "❌ Please provide a date in format YYYYMM (e.g., 202504)"
  exit 1
fi

year=${1:0:4}
month=${1:4:2}

# Calculate how many days are in the month
days_in_month=$(cal "$month" "$year" | awk 'NF {DAYS = $NF}; END {print DAYS}')

echo "📅 Running sequential enrichment for $year-$month with $days_in_month days..."

for day in $(seq -w 1 $days_in_month); do
  full_date="${year}${month}${day}"
  echo "➡️  Starting enrichment for $full_date"
  ./start_enrich.sh "$full_date"
  echo "✅ Finished enrichment for $full_date"
done
