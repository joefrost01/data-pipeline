# ------------------------------------------------------------------------------
# Pub/Sub Topics and Subscriptions
# ------------------------------------------------------------------------------

# High-volume streaming bridge topic
resource "google_pubsub_topic" "rfq_stream" {
  name   = "${local.name_prefix}-rfq-stream"
  labels = local.labels

  message_retention_duration = "86400s" # 24 hours
}

resource "google_pubsub_subscription" "rfq_stream_bq" {
  name   = "${local.name_prefix}-rfq-stream-bq"
  topic  = google_pubsub_topic.rfq_stream.id
  labels = local.labels

  bigquery_config {
    table            = "${var.project_id}.${google_bigquery_dataset.raw.dataset_id}.${google_bigquery_table.rfq_stream.table_id}"
    use_table_schema = true
    write_metadata   = true
  }

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s" # 7 days
  retain_acked_messages      = false

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 5
  }
}

# Regulatory events topic
resource "google_pubsub_topic" "regulatory_events" {
  name   = "${local.name_prefix}-regulatory-events"
  labels = local.labels

  message_retention_duration = "86400s"
}

resource "google_pubsub_subscription" "regulatory_events_push" {
  name   = "${local.name_prefix}-regulatory-events-push"
  topic  = google_pubsub_topic.regulatory_events.id
  labels = local.labels

  push_config {
    push_endpoint = google_cloud_run_service.regulatory_reporter.status[0].url

    oidc_token {
      service_account_email = google_service_account.pubsub_invoker.email
    }
  }

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"

  retry_policy {
    minimum_backoff = "1s"
    maximum_backoff = "60s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.regulatory_dead_letter.id
    max_delivery_attempts = 5
  }
}

# Dead letter topics
resource "google_pubsub_topic" "dead_letter" {
  name   = "${local.name_prefix}-dead-letter"
  labels = local.labels

  message_retention_duration = "604800s" # 7 days
}

resource "google_pubsub_topic" "regulatory_dead_letter" {
  name   = "${local.name_prefix}-regulatory-dead-letter"
  labels = local.labels

  message_retention_duration = "604800s"
}

resource "google_pubsub_subscription" "dead_letter_pull" {
  name   = "${local.name_prefix}-dead-letter-pull"
  topic  = google_pubsub_topic.dead_letter.id
  labels = local.labels

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
}

resource "google_pubsub_subscription" "regulatory_dead_letter_pull" {
  name   = "${local.name_prefix}-regulatory-dead-letter-pull"
  topic  = google_pubsub_topic.regulatory_dead_letter.id
  labels = local.labels

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
}
