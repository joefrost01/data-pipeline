# Environment Promotion Guide

How to promote changes from int to prod environment.

## Environment Overview

| Environment | Purpose | Project ID           | Who Can Deploy |
|-------------|---------|----------------------|----------------|
| `int` | Integration testing, UAT | `markets-int-12345`  | All engineers |
| `prod` | Production | `markets-prod-12345` | Release Manager only |

## Promotion Workflow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Feature   │────▶│     int     │────▶│    prod     │
│   Branch    │     │  (testing)  │     │ (production)│
└─────────────┘     └─────────────┘     └─────────────┘
      │                   │                   │
      │                   │                   │
    PR to main      Automated         Release PR
    + Code Review   deployment        + Change Record
```

## Step 1: Develop and Test in int

All development happens against the `int` environment.

1. Create feature branch from `main`
2. Make changes (dbt models, source specs, orchestrator code)
3. Test locally against int:
   ```bash
   export DBT_TARGET=int
   dbt run --select your_model
   ```
4. Create PR to `main`
5. CI runs:
   - Lint checks
   - Unit tests
   - dbt compile
   - Namespace validation
6. Code review and approval
7. Merge to `main` → Auto-deploys to int

## Step 2: Validate in int

After merge to main:

1. Verify deployment:
   ```bash
   kubectl get pods -n markets
   kubectl logs -n markets deploy/surveillance-pipeline --tail=50
   ```

2. Wait for at least one full pipeline cycle (1 hour)

3. Validate data:
   ```sql
   -- Check your model ran successfully
   SELECT * FROM control.dbt_runs
   WHERE model_name LIKE '%your_model%'
   ORDER BY run_timestamp DESC
   LIMIT 5
   ```

4. Run smoke tests if applicable:
   ```bash
   make test-source SOURCE=your_source
   ```

5. Get sign-off from:
   - Data Engineering (technical correctness)
   - Business stakeholder (data quality)

## Step 3: Promote to prod

**Prerequisites:**
- Changes validated in int for minimum 24 hours
- No open P1/P2 incidents related to changes
- Change Record approved in ServiceNow
- Release Manager available

### Option A: Terraform Changes (Infrastructure)

If your changes include Terraform modifications:

1. Create `terraform/prod/` directory (mirror of `int/`):
   ```bash
   # First time setup only
   cp -r terraform/int terraform/prod
   # Update terraform.tfvars with prod values
   ```

2. Update `terraform/prod/terraform.tfvars`:
   ```hcl
   project_id                   = "markets-prod-12345"
   region                       = "europe-west2"
   surveillance_partner_project = "partner-surveillance-prod"
   kafka_brokers                = "kafka-prod.internal:9092"
   regulator_api_url            = "https://api.regulator.example.com/v1/submit"
   ```

3. Plan and apply:
   ```bash
   cd terraform/prod
   terraform init
   terraform plan -out=tfplan
   
   # Review plan carefully
   
   # Apply (requires Release Manager approval)
   terraform apply tfplan
   ```

### Option B: Application Changes Only (dbt, orchestrator)

1. Create release PR:
   ```bash
   git checkout main
   git pull
   git checkout -b release/v1.2.3
   
   # Update version in pyproject.toml if needed
   # Create CHANGELOG entry
   
   git commit -m "Release v1.2.3"
   git push -u origin release/v1.2.3
   ```

2. Create PR targeting `prod` branch (if using branch-based deployment)

3. Or, trigger prod deployment manually:
   ```bash
   # Build and push prod image
   docker build -t gcr.io/markets-prod-12345/markets-orchestrator:v1.2.3 .
   docker push gcr.io/markets-prod-12345/markets-orchestrator:v1.2.3
   
   # Update K8s deployment
   kubectl set image -n markets \
     cronjob/markets-pipeline \
     orchestrator=gcr.io/markets-prod-12345/markets-orchestrator:v1.2.3
   ```

4. Verify deployment:
   ```bash
   # Check next run succeeds
   kubectl get jobs -n markets --watch
   ```

## Rollback Procedure

If issues are discovered in prod:

### Quick Rollback (Application)

```bash
# Find previous working image
gcloud container images list-tags gcr.io/markets-prod-12345/markets-orchestrator

# Rollback to previous version
kubectl set image -n markets \
  cronjob/markets-pipeline \
  orchestrator=gcr.io/markets-prod-12345/markets-orchestrator:v1.2.2
```

### dbt Model Rollback

For dbt changes, you may need to:

1. Revert the model code
2. Run a full refresh to restore previous state:
   ```bash
   kubectl exec -it -n markets deploy/markets-pipeline -- \
     dbt run --select affected_model --full-refresh
   ```

### Terraform Rollback

```bash
cd terraform/prod

# Revert to previous commit
git checkout HEAD~1 -- .

# Plan and apply
terraform plan -out=rollback.tfplan
terraform apply rollback.tfplan
```

## Change Freeze Periods

No production deployments during:
- Month-end close (last 3 business days of month)
- Year-end close (Dec 20 - Jan 5)
- Major regulatory filing dates (check calendar)
- Bank holidays

Emergency fixes require:
- VP approval
- On-call engineer present
- Documented rollback plan

## Environment Parity

Keep int and prod as similar as possible:

| Aspect | int | prod |
|--------|-----|------|
| GKE cluster size | 3 nodes | 3 nodes (auto-scale to 10) |
| BigQuery location | europe-west2 | europe-west2 |
| Retention periods | Same | Same |
| dbt models | Identical | Identical |
| Source specs | Identical | Identical |

**Differences:**
- Project IDs
- Service account emails
- Kafka broker addresses
- API endpoints (UAT vs prod)
- Alert destinations

## Checklist: Before Promoting to Prod

- [ ] Changes tested in int for 24+ hours
- [ ] At least 2 full pipeline cycles completed successfully
- [ ] Data quality validated by business stakeholder
- [ ] No increase in quarantined rows
- [ ] Performance metrics stable (check Dynatrace)
- [ ] Change Record created and approved
- [ ] Rollback plan documented
- [ ] Release Manager notified
- [ ] Not in change freeze period
