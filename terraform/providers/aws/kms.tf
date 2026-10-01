resource "aws_kms_key" "lakehouse" {
  description             = "Encrypts the parity S3 bucket and stands in for the key real Kestra secrets would use"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Name = "${var.project_name}-kms"
  }
}

resource "aws_kms_alias" "lakehouse" {
  name          = "alias/${var.project_name}"
  target_key_id = aws_kms_key.lakehouse.key_id
}
