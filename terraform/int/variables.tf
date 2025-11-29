variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west2"
}

variable "zone" {
  description = "GCP zone for zonal resources"
  type        = string
  default     = "europe-west2-a"
}

variable "surveillance_partner_project" {
  description = "GCP project ID for surveillance partner (extract access)"
  type        = string
}

variable "surveillance_partner_service_account" {
  description = "Full service account email for surveillance partner reader access"
  type        = string
  default     = ""  # If empty, will be constructed from partner project
}

variable "kafka_brokers" {
  description = "Kafka bootstrap servers"
  type        = string
}

variable "regulator_api_url" {
  description = "Regulatory submission API endpoint"
  type        = string
}

variable "enable_deletion_protection" {
  description = "Enable deletion protection on critical resources"
  type        = bool
  default     = true
}

variable "terraform_state_lock" {
  description = "Enable state locking (should always be true for prod)"
  type        = bool
  default     = true
}
