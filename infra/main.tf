resource "aws_iam_role" "lambda" {
  name = "${var.project_name}-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "basic_execution" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "bot" {
  name              = "/aws/lambda/${var.project_name}"
  retention_in_days = 14
}

resource "aws_lambda_function" "bot" {
  function_name = var.project_name
  role          = aws_iam_role.lambda.arn
  filename      = var.lambda_zip_path

  source_code_hash = filebase64sha256(var.lambda_zip_path)
  runtime          = "provided.al2023"
  handler          = "bootstrap"
  architectures    = ["arm64"]

  memory_size = var.memory_size_mb
  timeout     = var.timeout_seconds

  ephemeral_storage {
    size = var.ephemeral_storage_mb
  }

  environment {
    variables = {
      TELEGRAM_BOT_TOKEN             = var.telegram_bot_token
      TELEGRAM_WEBHOOK_SECRET        = var.telegram_webhook_secret
      ALLOWED_TELEGRAM_USER_IDS      = var.allowed_telegram_user_ids
      DEFAULT_WIKI_LANGUAGE          = var.default_wiki_language
      FAVORITE_CATEGORIES            = var.favorite_categories
      SEARCH_LIMIT                   = tostring(var.search_limit)
      TELEGRAM_MESSAGE_CHAR_LIMIT    = tostring(var.telegram_message_char_limit)
      BIG_ARTICLE_CHAR_THRESHOLD     = tostring(var.big_article_char_threshold)
      SMALL_ARTICLE_CHAR_THRESHOLD   = tostring(var.small_article_char_threshold)
      METADATA_REVISION_LIMIT        = tostring(var.metadata_revision_limit)
      WIKIPEDIA_HTTP_TIMEOUT_SECONDS = tostring(var.wikipedia_http_timeout_seconds)
      RAM_CACHE_MAX_ENTRIES          = tostring(var.ram_cache_max_entries)
      ARTICLE_IMAGES_BUTTON_ONLY     = tostring(var.article_images_button_only)
      RUST_LOG                       = "telegram_wikipedia_bot=info"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.bot,
    aws_iam_role_policy_attachment.basic_execution
  ]
}

resource "aws_lambda_function_url" "bot" {
  function_name      = aws_lambda_function.bot.function_name
  authorization_type = "NONE"
}

resource "aws_lambda_permission" "allow_function_url" {
  statement_id           = "AllowPublicFunctionUrlInvoke"
  action                 = "lambda:InvokeFunctionUrl"
  function_name          = aws_lambda_function.bot.function_name
  principal              = "*"
  function_url_auth_type = "NONE"
}

resource "aws_lambda_permission" "allow_function_url_invoke_function" {
  statement_id             = "AllowPublicFunctionUrlInvokeFunction"
  action                   = "lambda:InvokeFunction"
  function_name            = aws_lambda_function.bot.function_name
  principal                = "*"
  invoked_via_function_url = true
}
