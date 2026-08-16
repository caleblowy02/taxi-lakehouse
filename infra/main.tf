# main.tf
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = "us-east-1"
}

# One bucket, folder-per-layer (simplest, cheapest for a portfolio project)
resource "aws_s3_bucket" "lakehouse" {
  bucket = "taxi-lakehouse-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration { status = "Enabled" }
}

data "aws_caller_identity" "current" {}

# IAM role Databricks assumes to read/write S3 (instance profile pattern)
resource "aws_iam_role" "databricks_s3_access" {
  name = "databricks-taxi-lakehouse-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        AWS = [
          "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/databricks-taxi-lakehouse-role"
        ]
      }
      Action = "sts:AssumeRole"
      Condition = { StringEquals = { "sts:ExternalId" = "9ffe3612-f409-4e39-8182-38b294fcc946" } }
    }]
  })
}

resource "aws_iam_role_policy" "s3_access" {
  name = "s3-lakehouse-access"
  role = aws_iam_role.databricks_s3_access.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.lakehouse.arn,
        "${aws_s3_bucket.lakehouse.arn}/*"
      ]
    }]
  })
}

variable "databricks_account_id" {
  description = "Your Databricks account ID (Account Console > Settings)"
  type        = string
}

output "bucket_name" {
  value = aws_s3_bucket.lakehouse.bucket
}