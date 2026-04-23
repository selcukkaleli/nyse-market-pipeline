terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.42.0"
    }
  }
}

provider "aws" {
  region     = var.region
}

resource "aws_s3_bucket" "raw_data" {
  bucket = "${var.project_id}-raw-data"

  tags = {
    Name        = "Raw"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket" "analytics_data" {
  bucket = "${var.project_id}-analytics-data"

  tags = {
    Name        = "Analytics_Data"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project_id}-athena-results"

  tags = {
    Name        = "Athena_Results"
    Environment = "Dev"
  }
}

resource "aws_glue_catalog_database" "nyse_raw" {
  name = "nyse_raw"
}

resource "aws_athena_workgroup" "nyse_workgroup" {
  name = "nyse_workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/output/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}