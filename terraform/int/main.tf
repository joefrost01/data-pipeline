terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
  }

  # State configuration with locking
  # GCS backend automatically uses object locking for state
  backend "gcs" {
    bucket = "markets-terraform-state"
    prefix = "int"
    
    # State locking is automatic with GCS backend
    # No additional configuration needed
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

locals {
  env         = "int"
  name_prefix = "markets-${local.env}"

  # Standard labels for all resources
  labels = {
    environment = local.env
    application = "markets-pipeline"
    managed_by  = "terraform"
    cost_centre = "data-engineering"
  }

}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "container.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ])
  
  service            = each.value
  disable_on_destroy = false
}
