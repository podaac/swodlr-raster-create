# -- SNS --
// This is mapped from the Terraform infrastructure defined in the
// podaac/swodlr-api repo
data "aws_sns_topic" "product_update" {
  name = "${local.app_prefix}-product-update-topic"
}

# -- SQS --
// This is mapped from the Terraform infrastructure defined in the
// podaac/swodlr-api repo
data "aws_sqs_queue" "product_create" {
  name = "${local.app_prefix}-product-create-queue"
}

# -- Event Mapping --
resource "aws_lambda_event_source_mapping" "product_create_queue" {
  event_source_arn = data.aws_sqs_queue.product_create.arn
  function_name = aws_lambda_function.bootstrap.arn
  batch_size = 1  # Disable premature optimizations for now
}
