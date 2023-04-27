# -- Lambdas --
resource "aws_lambda_function" "bootstrap" {
  function_name = "${local.service_prefix}-bootstrap"
  handler = "podaac.swodlr_ingest_to_sds.bootstrap.lambda_handler"

  role = aws_iam_role.bootstrap.arn
  runtime = "python3.9"

  filename = "${path.module}/../dist/${local.name}-${local.version}.zip"
  source_code_hash = filebase64sha256("${path.module}/../dist/${local.name}-${local.version}.zip")
}

resource "aws_lambda_function" "notify_update" {
  function_name = "${local.service_prefix}-notify_update"
  handler = "podaac.swodlr_ingest_to_sds.notify_update.lambda_handler"

  role = aws_iam_role.lambda.arn
  runtime = "python3.9"

  filename = "${path.module}/../dist/${local.name}-${local.version}.zip"
  source_code_hash = filebase64sha256("${path.module}/../dist/${local.name}-${local.version}.zip")
}

resource "aws_lambda_function" "submit_evaluate" {
  function_name = "${local.service_prefix}-submit_evaluate"
  handler = "podaac.swodlr_ingest_to_sds.submit_evaluate.lambda_handler"

  role = aws_iam_role.lambda.arn
  runtime = "python3.9"

  filename = "${path.module}/../dist/${local.name}-${local.version}.zip"
  source_code_hash = filebase64sha256("${path.module}/../dist/${local.name}-${local.version}.zip")

  vpc_config {
    security_group_ids = [aws_security_group.default.id]
    subnet_ids = data.aws_subnets.private.ids
  }
}

resource "aws_lambda_function" "submit_raster" {
  function_name = "${local.service_prefix}-submit_raster"
  handler = "podaac.swodlr_ingest_to_sds.submit_raster.lambda_handler"

  role = aws_iam_role.lambda.arn
  runtime = "python3.9"

  filename = "${path.module}/../dist/${local.name}-${local.version}.zip"
  source_code_hash = filebase64sha256("${path.module}/../dist/${local.name}-${local.version}.zip")

  vpc_config {
    security_group_ids = [aws_security_group.default.id]
    subnet_ids = data.aws_subnets.private.ids
  }
}

resource "aws_lambda_function" "wait_for_complete" {
  function_name = "${local.service_prefix}-wait_for_complete"
  handler = "podaac.swodlr_ingest_to_sds.wait_for_complete.lambda_handler"

  role = aws_iam_role.lambda.arn
  runtime = "python3.9"

  filename = "${path.module}/../dist/${local.name}-${local.version}.zip"
  source_code_hash = filebase64sha256("${path.module}/../dist/${local.name}-${local.version}.zip")

  vpc_config {
    security_group_ids = [aws_security_group.default.id]
    subnet_ids = data.aws_subnets.private.ids
  }
}

# -- IAM --
resource "aws_iam_policy" "ssm_parameters_read" {
  name_prefix = "SSMParametersReadOnlyAccess"
  path = "${local.service_path}/"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid = ""
      Action = [
        "ssm:GetParameter",
        "ssm:GetParameters",
        "ssm:GetParametersByPath"
      ]
      Effect   = "Allow"
      Resource = "arn:aws:ssm:${var.region}:${local.account_id}:parameter${local.service_path}/*"
    }]
  })
}

resource "aws_iam_policy" "lambda_networking" {
  name_prefix = "LambdaNetworkAccess"
  path = "${local.service_path}/"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = [
        "ec2:DescribeInstances",
        "ec2:CreateNetworkInterface",
        "ec2:AttachNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DeleteNetworkInterface"
      ]
      Effect   = "Allow"
      Resource = "*"
    }]
  })
}

resource "aws_iam_role" "bootstrap" {
  name_prefix = "bootstrap"
  path = "${local.service_path}/"

  permissions_boundary = "arn:aws:iam::${local.account_id}:policy/NGAPShRoleBoundary"
  managed_policy_arns = [
    "arn:aws:iam::${local.account_id}:policy/NGAPProtAppInstanceMinimalPolicy",
    aws_iam_policy.ssm_parameters_read.arn
  ]

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })

  inline_policy {
    name = "BootstrapPolicy"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Sid = ""
          Action = "states:StartExecution"
          Effect   = "Allow"
          Resource = aws_sfn_state_machine.raster_create.arn
        },

        {
          Sid = ""
          Action = [
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes"
          ]
          Effect   = "Allow"
          Resource = data.aws_sqs_queue.product_create.arn
        }
      ]
    })
  }
}

resource "aws_iam_role" "lambda" {
  name_prefix = "lambda"
  path = "${local.service_path}/"

  permissions_boundary = "arn:aws:iam::${local.account_id}:policy/NGAPShRoleBoundary"
  managed_policy_arns = [
    "arn:aws:iam::${local.account_id}:policy/NGAPProtAppInstanceMinimalPolicy",
    aws_iam_policy.ssm_parameters_read.arn,
    aws_iam_policy.lambda_networking.arn
  ]

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

# -- SSM Parameters --
resource "aws_ssm_parameter" "sds_pcm_release_tag" {
  name  = "${local.service_path}/sds_pcm_release_tag"
  type  = "String"
  overwrite = true
  value = var.sds_pcm_release_tag
}

resource "aws_ssm_parameter" "sds_host" {
  name  = "${local.service_path}/sds_host"
  type  = "String"
  overwrite = true
  value = var.sds_host
}

resource "aws_ssm_parameter" "sds_username" {
  name  = "${local.service_path}/sds_username"
  type  = "String"
  overwrite = true
  value = var.sds_username
}

resource "aws_ssm_parameter" "sds_password" {
  name  = "${local.service_path}/sds_password"
  type  = "SecureString"
  overwrite = true
  value = var.sds_password
}

resource "aws_ssm_parameter" "sds_ca_cert" {
  name = "${local.service_path}/sds_ca_cert"
  type = "SecureString"
  overwrite = true
  value = local.sds_ca_cert
}

resource "aws_ssm_parameter" "sds_submit_max_attempts" {
  name = "${local.service_path}/sds_submit_max_attempts"
  type = "String"
  overwrite = true
  value = var.sds_submit_max_attempts
}

resource "aws_ssm_parameter" "sds_submit_timeout" {
  name = "${local.service_path}/sds_submit_timeout"
  type = "String"
  overwrite = true
  value = var.sds_submit_timeout
}

resource "aws_ssm_parameter" "stepfunction_arn" {
  name  = "${local.service_path}/stepfunction_arn"
  type  = "String"
  overwrite = true
  value = aws_sfn_state_machine.raster_create.arn
}

resource "aws_ssm_parameter" "update_max_attempts" {
  name  = "${local.service_path}/update_max_attempts"
  type  = "String"
  overwrite = true
  value = var.update_max_attempts
}

resource "aws_ssm_parameter" "update_topic_arn" {
  name  = "${local.service_path}/update_topic_arn"
  type  = "String"
  overwrite = true
  value = data.aws_sns_topic.product_update.arn
}
