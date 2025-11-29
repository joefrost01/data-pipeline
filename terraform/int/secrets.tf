# ------------------------------------------------------------------------------
# Secret Manager
# ------------------------------------------------------------------------------

resource "google_secret_manager_secret" "kafka_credentials" {
  secret_id = "${local.name_prefix}-kafka-credentials"
  labels    = local.labels

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "regulator_api_key" {
  secret_id = "${local.name_prefix}-regulator-api-key"
  labels    = local.labels

  replication {
    auto {}
  }
}

# Note: Secret values are not managed in Terraform
# Populate manually or via CI/CD:
#   gcloud secrets versions add markets-int-kafka-credentials --data-file=kafka.json
#   gcloud secrets versions add markets-int-regulator-api-key --data-file=api-key.txt
