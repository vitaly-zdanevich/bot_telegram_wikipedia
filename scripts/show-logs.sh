#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="${PROJECT_NAME:-telegram-wikipedia-bot}"
AWS_REGION="${AWS_REGION:-us-east-1}"

aws logs tail "/aws/lambda/$PROJECT_NAME" \
  --region "$AWS_REGION" \
  --follow \
  --format short
