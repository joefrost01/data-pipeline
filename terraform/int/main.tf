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

  backend "gcs" {
    bucket = "surveillance-terraform-state"
    prefix = "int"
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
  name_prefix = "surveillance-${local.env}"

  # Standard labels for all resources
  labels = {
    environment = local.env
    application = "surveillance-pipeline"
    managed_by  = "terraform"
  }
}
