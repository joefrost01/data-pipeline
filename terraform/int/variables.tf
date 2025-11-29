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

variable "kafka_brokers" {
  description = "Kafka bootstrap servers"
  type        = string
}

variable "regulator_api_url" {
  description = "Regulatory submission API endpoint"
  type        = string
}
