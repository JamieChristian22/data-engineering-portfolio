output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}
output "glue_database" {
  value = aws_glue_catalog_database.lake_db.name
}
output "athena_workgroup" {
  value = aws_athena_workgroup.wg.name
}
