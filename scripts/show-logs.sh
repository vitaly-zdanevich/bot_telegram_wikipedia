#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="${PROJECT_NAME:-telegram-wikipedia-bot}"
AWS_REGION="${AWS_REGION:-us-east-1}"
START_MINUTES="${START_MINUTES:-20}"
LOG_LIMIT="${LOG_LIMIT:-200}"

if aws logs tail help >/dev/null 2>&1; then
  exec aws logs tail "/aws/lambda/$PROJECT_NAME" \
    --region "$AWS_REGION" \
    --follow \
    --format short
fi

start_time_ms="$(($(date +%s) * 1000 - START_MINUTES * 60 * 1000))"
echo "aws logs tail is unavailable; showing the last ${START_MINUTES} minute(s)." >&2

aws logs filter-log-events \
  --log-group-name "/aws/lambda/$PROJECT_NAME" \
  --region "$AWS_REGION" \
  --start-time "$start_time_ms" \
  --limit "$LOG_LIMIT" \
  --output text \
  --query 'events[*].[timestamp,message]'
