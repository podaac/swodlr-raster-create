# -- Step Function --
resource "aws_sfn_state_machine" "raster_create" {
  name = local.service_prefix
  role_arn = aws_iam_role.sfn.arn

  definition = jsonencode({
    StartAt = "SubmitEvaluate"
    States = {
      SubmitEvaluate = {
        Type = "Task"
        Resource = aws_lambda_function.submit_evaluate.arn
        Next = "WaitForEvaluateComplete"
      }

      WaitForEvaluateComplete = {
        Type = "Task"
        Resource = aws_lambda_function.wait_for_complete.arn
        Next = "CheckEvaluateJobs"
      }

      CheckEvaluateJobs = {
        Type = "Choice",
        Choices = [{
          And = [
            {
              Variable = "$.waiting"
              IsPresent = true
            },
            {
              Variable = "$.waiting"
              BooleanEquals = true
            }
          ]
          Next = "TimeoutEvaluate"
        }]
        Default = "SubmitRaster"
      }

      TimeoutEvaluate = {
        Type = "Wait"
        Seconds = 60
        Next = "WaitForEvaluateComplete"
      }

      SubmitRaster = {
        Type = "Task"
        Resource = aws_lambda_function.submit_raster.arn
        Next = "WaitForRasterComplete"
      }

      WaitForRasterComplete = {
        Type = "Task"
        Resource = aws_lambda_function.wait_for_complete.arn
        Next = "CheckRasterJobs"
      }

      CheckRasterJobs = {
        Type = "Choice",
        Choices = [{
          And = [
            {
              Variable = "$.waiting"
              IsPresent = true
            },
            {
              Variable = "$.waiting"
              BooleanEquals = true
            }
          ]
          Next = "TimeoutRaster"
        }]
        Default = "NotifyUpdate"
      }

      TimeoutRaster = {
        Type = "Wait"
        Seconds = 60
        Next = "WaitForRasterComplete"
      }

      NotifyUpdate = {
        Type = "Task"
        Resource = aws_lambda_function.notify_update.arn
        Next = "Done"
      }

      Done = {
        Type = "Succeed"
      }
    }
  })
}

# -- IAM --
resource "aws_iam_role" "sfn" {
  name_prefix = "sfn"
  path = "${local.service_path}/"

  permissions_boundary = "arn:aws:iam::${local.account_id}:policy/NGAPShRoleBoundary"
  managed_policy_arns = [
    "arn:aws:iam::${local.account_id}:policy/NGAPProtAppInstanceMinimalPolicy"
  ]

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "states.amazonaws.com"
      }
    }]
  })

  inline_policy {
    name = "LambdasExecute"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [{
        Sid = ""
        Action = "lambda:InvokeFunction"
        Effect   = "Allow"
        Resource = [
          aws_lambda_function.notify_update.arn,
          aws_lambda_function.submit_evaluate.arn,
          aws_lambda_function.submit_raster.arn,
          aws_lambda_function.wait_for_complete.arn
        ]
      }]
    })
  }
}
