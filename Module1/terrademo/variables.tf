variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
}
variable "project" {
  description = "Project"
  default     = "terraform-demo-486218"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}
variable "location" {
  description = "Project location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket name"
  default     = "terraform-demo-bucket-486218-terra-bucket"

}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}