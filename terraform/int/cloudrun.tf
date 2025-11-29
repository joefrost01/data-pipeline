# ------------------------------------------------------------------------------
# Cloud Run - Regulatory Reporter
# ------------------------------------------------------------------------------

resource "google_cloud_run_service" "regulatory_reporter" {
  name     = "${local.name_prefix}-regulatory-reporter"
  location = var.region

  template {
    spec {
      service_account_name = google_service_account.regulatory_reporter.email

      containers {
        image = "gcr.io/${var.project_id}/regulatory-reporter:latest"

        ports {
          container_port = 8080
        }

        env {
          name  = "PROJECT_ID"
          value = var.project_id
        }

        env {
          name  = "REGULATOR_API_URL"
          value = var.regulator_api_url
        }

        env {
          name = "REGULATOR_API_KEY"
          value_from {
            secret_key_ref {
              name = google_secret_manager_secret.regulator_api_key.secret_id
              key  = "latest"
            }
          }
        }

        env {
          name  = "CACHE_REFRESH_SECONDS"
          value = "300"
        }

        resources {
          limits = {
            cpu    = "1000m"
            memory = "512Mi"
          }
        }
      }

      container_concurrency = 80
      timeout_seconds       = 60
    }

    metadata {
      annotations = {
        "autoscaling.knative.dev/minScale" = "1"  # Keep warm for latency
        "autoscaling.knative.dev/maxScale" = "10"
        "run.googleapis.com/cpu-throttling" = "false"
      }

      labels = local.labels
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  autogenerate_revision_name = true

  lifecycle {
    ignore_changes = [
      template[0].metadata[0].annotations["client.knative.dev/user-image"],
      template[0].metadata[0].annotations["run.googleapis.com/client-name"],
      template[0].metadata[0].annotations["run.googleapis.com/client-version"],
    ]
  }
}

# No public access - only Pub/Sub can invoke
resource "google_cloud_run_service_iam_member" "noauth" {
  service  = google_cloud_run_service.regulatory_reporter.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pubsub_invoker.email}"
}
