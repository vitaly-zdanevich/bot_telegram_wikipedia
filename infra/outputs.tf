output "function_url" {
  description = "Public Lambda Function URL. Use this as the Telegram webhook URL."
  value       = aws_lambda_function_url.bot.function_url
}

output "function_name" {
  description = "Lambda function name."
  value       = aws_lambda_function.bot.function_name
}

output "log_group_name" {
  description = "CloudWatch Logs group for the Lambda function."
  value       = aws_cloudwatch_log_group.bot.name
}

output "webhook_info_command" {
  description = "Command to inspect Telegram webhook delivery status."
  value       = "curl -s \"https://api.telegram.org/bot$TF_VAR_telegram_bot_token/getWebhookInfo\""
}

output "set_webhook_hint" {
  description = "Command shape for registering the Telegram webhook."
  value       = "TELEGRAM_BOT_TOKEN=... TELEGRAM_WEBHOOK_SECRET=... FUNCTION_URL=${aws_lambda_function_url.bot.function_url} ../scripts/set-webhook.sh"
  sensitive   = true
}
