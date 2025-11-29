# Surveillance Pipeline Makefile
# 
# Common operations for development and testing

.PHONY: help install test lint validate-specs test-source integration-test clean

# Default target
help:
	@echo "Surveillance Pipeline - Available targets:"
	@echo ""
	@echo "  install          Install all dependencies"
	@echo "  test             Run all tests"
	@echo "  lint             Run linters"
	@echo "  validate-specs   Validate all source specification YAML files"
	@echo "  test-source      Smoke test a source (SOURCE=name required)"
	@echo "  integration-test Run full integration test suite"
	@echo "  dbt-compile      Compile dbt project"
	@echo "  dbt-test         Run dbt tests"
	@echo "  clean            Clean build artifacts"
	@echo ""
	@echo "Examples:"
	@echo "  make test-source SOURCE=murex_trades"
	@echo "  make integration-test"

# Install dependencies
install:
	pip install -e "orchestrator/[dev]"
	pip install dbt-core dbt-bigquery
	cd dbt_project && dbt deps

# Run all Python tests
test:
	pytest orchestrator/tests/ -v --tb=short

# Run linters
lint:
	ruff check orchestrator/ regulatory_reporter/ streaming/
	mypy orchestrator/ --ignore-missing-imports

# Validate source specs
validate-specs:
	@python -c "\
import yaml; \
from pathlib import Path; \
errors = []; \
[errors.append(f'{f}: {e}') for f in Path('source_specs').rglob('*.yaml') \
 for e in (lambda s: [f'Missing {r}' for r in ['name','source','schema'] if r not in s])(yaml.safe_load(open(f)))]; \
print('✓ All specs valid' if not errors else '\\n'.join(['❌ Errors:'] + errors)); \
exit(1 if errors else 0)"

# Validate a specific source spec
validate-spec:
ifndef SOURCE
	$(error SOURCE is required. Usage: make validate-spec SOURCE=murex_trades)
endif
	@python -c "\
import yaml; \
spec = yaml.safe_load(open('source_specs/trading/$(SOURCE).yaml')); \
required = ['name', 'source', 'schema']; \
missing = [r for r in required if r not in spec]; \
print('✓ Valid' if not missing else f'❌ Missing: {missing}'); \
exit(1 if missing else 0)"

# Smoke test a source end-to-end
test-source:
ifndef SOURCE
	$(error SOURCE is required. Usage: make test-source SOURCE=murex_trades)
endif
ifndef PROJECT_ID
	$(eval PROJECT_ID := surveillance-int-12345)
endif
	@echo "Running smoke test for source: $(SOURCE)"
	@echo "Project: $(PROJECT_ID)"
	@echo ""
	@echo "Step 1: Generate test data..."
	python scripts/generate_test_data.py --source $(SOURCE) --output /tmp/test_$(SOURCE).csv
	@echo ""
	@echo "Step 2: Upload to landing bucket..."
	gsutil cp /tmp/test_$(SOURCE).csv gs://$(PROJECT_ID)-landing/$(SOURCE)/test_$(shell date +%Y%m%d%H%M%S).csv
	@echo ""
	@echo "Step 3: Trigger pipeline run..."
	kubectl create job --from=cronjob/surveillance-pipeline smoke-test-$(shell date +%s) -n surveillance
	@echo ""
	@echo "Step 4: Waiting for completion (timeout 10m)..."
	kubectl wait --for=condition=complete job -l app=surveillance-pipeline -n surveillance --timeout=600s
	@echo ""
	@echo "Step 5: Verify data in BigQuery..."
	bq query --use_legacy_sql=false \
		"SELECT COUNT(*) as count FROM staging.stg_$(SOURCE) WHERE source_trade_id LIKE 'TEST-%'"
	@echo ""
	@echo "Step 6: Cleanup test data..."
	bq query --use_legacy_sql=false \
		"DELETE FROM curation.trades_enriched WHERE source_trade_id LIKE 'TEST-%' AND source_system = '$(shell echo $(SOURCE) | tr '[:lower:]' '[:upper:]')'"
	@echo ""
	@echo "✓ Smoke test complete"

# Full integration test
integration-test:
ifndef PROJECT_ID
	$(eval PROJECT_ID := surveillance-int-12345)
endif
	@echo "Running full integration test against $(PROJECT_ID)"
	@echo ""
	pytest orchestrator/tests/integration/ -v --tb=short

# Compile dbt project
dbt-compile:
	cd dbt_project && dbt compile

# Run dbt tests
dbt-test:
	cd dbt_project && dbt test

# Run dbt build (models + tests)
dbt-build:
	cd dbt_project && dbt build

# Clean build artifacts
clean:
	rm -rf dbt_project/target/
	rm -rf dbt_project/dbt_packages/
	rm -rf orchestrator/.pytest_cache/
	rm -rf orchestrator/*.egg-info/
	rm -rf __pycache__/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

# Docker build
docker-build:
	docker build -t surveillance-orchestrator:latest -f orchestrator/Dockerfile .

# Deploy to int environment
deploy-int:
	@echo "Deploying to int environment..."
	docker build -t gcr.io/surveillance-int-12345/surveillance-orchestrator:latest -f orchestrator/Dockerfile .
	docker push gcr.io/surveillance-int-12345/surveillance-orchestrator:latest
	kubectl rollout restart cronjob/surveillance-pipeline -n surveillance
	@echo "✓ Deployed"
