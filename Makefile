# Convenience targets. Python 3.10+ stdlib only, no pip dependencies.

PYTHON ?= python3
SERVICE_URL ?= http://localhost:8080

.PHONY: help run test load-50k stress restart-check example smoke baseline burst offline adversarial

help:
	@echo "Targets:"
	@echo "  run            start PostgreSQL and the service"
	@echo "  test           run focused domain checks"
	@echo "  load-50k       send 50,000 events and report throughput/p95"
	@echo "  stress         run a configurable burst and print metrics"
	@echo "  restart-check  hard-kill the app and verify recovery"
	@echo "  example        run the example stub service on :8080 (replace with yours)"
	@echo "  smoke          30s baseline run against \$$SERVICE_URL + scorecard"
	@echo "  baseline       60s baseline"
	@echo "  burst          3min run with two 10x bursts"
	@echo "  offline        2min run with 20% of devices going offline + replaying"
	@echo "  adversarial    4min run combining burst + offline + clock skew"
	@echo
	@echo "Override SERVICE_URL=... or DEVICES=... as needed."

DEVICES ?= 50

run:
	docker compose up --build

test:
	dotnet run --project tests/StreamingBackend.Tests

load-50k:
	dotnet run --project tools/LoadTest -c Release

stress:
	bash scripts/stress.sh

restart-check:
	bash scripts/restart-check.sh

example:
	$(PYTHON) example_solution/service.py

smoke:
	$(PYTHON) eval/check.py smoke --target $(SERVICE_URL) --devices $(DEVICES)

baseline:
	$(PYTHON) eval/check.py baseline --target $(SERVICE_URL) --devices $(DEVICES)

burst:
	$(PYTHON) eval/check.py burst --target $(SERVICE_URL) --devices $(DEVICES)

offline:
	$(PYTHON) eval/check.py offline --target $(SERVICE_URL) --devices $(DEVICES)

adversarial:
	$(PYTHON) eval/check.py adversarial --target $(SERVICE_URL) --devices $(DEVICES)
