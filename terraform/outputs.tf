output "raw_bucket_name" {
  description = "AWS bucket name"
  value       = aws_s3_bucket.raw_data.bucket
}

output "analytics_bucket_name" {
  description = "AWS bucket name"
  value       = aws_s3_bucket.analytics_data.bucket
}

output "athena_results_bucket_name" {
  description = "AWS bucket name"
  value       = aws_s3_bucket.athena_results.bucket
}

output "glue_catalog" {
  description = "Glue catalog it contains Glue Database (BQ Dataset) and Glue Table (BQ Ext table)"
  value       = aws_glue_catalog_database.nyse_raw.name
}

output "analytics_dataset" {
  description = "Athena Workgroup"
  value       = aws_athena_workgroup.nyse_workgroup.name
}