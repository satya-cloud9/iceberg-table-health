# S3 bucket created against floci. Originally a parity-only resource
# (validating the HCL against AWS's real API shape, nothing more) --
# promoted to the real, load-bearing shared object store once storage
# moved into the provider contract (see CONTRACT.md's "object-storage
# outputs" section). Nessie (terraform/platform) and every tenant's Trino
# now point at this bucket directly, isolated by path prefix per tenant.

resource "aws_s3_bucket" "parity" {
  bucket = var.storage_bucket_name

  tags = {
    Name = "${var.project_name}-aws-parity"
  }
}

resource "aws_s3_bucket_versioning" "parity" {
  bucket = aws_s3_bucket.parity.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "parity" {
  bucket = aws_s3_bucket.parity.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lakehouse.arn
    }
  }
}

resource "aws_s3_bucket_public_access_block" "parity" {
  bucket = aws_s3_bucket.parity.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
