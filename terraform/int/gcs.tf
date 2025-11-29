# ------------------------------------------------------------------------------
# GCS Buckets
# 
# IMPORTANT: force_destroy = false prevents accidental bucket deletion
# even when bucket contains objects. This is critical for data safety.
# ------------------------------------------------------------------------------

resource "google_storage_bucket" "landing" {
  name          = "${local.name_prefix}-landing"
  location      = var.region
  labels        = local.labels
  force_destroy = false  # CRITICAL: Prevent accidental deletion

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 90 # 90 days retention for landing
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "staging" {
  name          = "${local.name_prefix}-staging"
  location      = var.region
  labels        = local.labels
  force_destroy = false  # CRITICAL: Prevent accidental deletion

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = false
  }

  # Staging files are short-lived; archived after each run
  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "archive" {
  name          = "${local.name_prefix}-archive"
  location      = var.region
  labels        = local.labels
  force_destroy = false  # CRITICAL: Contains regulatory audit trail

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true # Protect against accidental deletion
  }

  # 7 years retention for regulatory compliance
  lifecycle_rule {
    condition {
      age = 2555 # ~7 years
    }
    action {
      type = "Delete"
    }
  }

  # Move to coldline after 90 days
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
}

resource "google_storage_bucket" "failed" {
  name          = "${local.name_prefix}-failed"
  location      = var.region
  labels        = local.labels
  force_destroy = false  # Preserve failed files for investigation

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true # Preserve failed files for investigation
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "extracts" {
  name          = "${local.name_prefix}-extracts"
  location      = var.region
  labels        = local.labels
  force_destroy = false  # Contains partner deliverables

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  # Keep extracts for 90 days; partner should have copied by then
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# Terraform state bucket (created manually or via bootstrap)
# This is referenced in backend config but defined here for documentation
# 
# resource "google_storage_bucket" "terraform_state" {
#   name          = "surveillance-terraform-state"
#   location      = var.region
#   force_destroy = false  # NEVER delete state bucket
#   
#   versioning {
#     enabled = true  # Required for state history
#   }
#   
#   uniform_bucket_level_access = true
# }
