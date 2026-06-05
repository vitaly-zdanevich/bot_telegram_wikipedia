#!/usr/bin/env bash
set -euo pipefail

: "${TELEGRAM_BOT_TOKEN:?Set TELEGRAM_BOT_TOKEN}"
: "${FUNCTION_URL:?Set FUNCTION_URL from terraform output function_url}"
: "${TELEGRAM_WEBHOOK_SECRET:?Set TELEGRAM_WEBHOOK_SECRET}"

WEBHOOK_URL="${FUNCTION_URL%/}/telegram"

curl -fsS \
  -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/setWebhook" \
  -d "url=${WEBHOOK_URL}" \
  -d "secret_token=${TELEGRAM_WEBHOOK_SECRET}" \
  -d 'allowed_updates=["message","callback_query","inline_query"]'

echo
echo "Webhook set to $WEBHOOK_URL"
