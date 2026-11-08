# Convenience targets. Python 3.10+ stdlib only, no pip dependencies.

PYTHON ?= python3
SERVICE_URL ?= http://localhost:8080

.PHONY: help run db-reset example smoke baseline burst offline adversarial

help:
	@echo "Targets:"
	@echo "  run            build and start Postgres + the service on :8080 (Docker Compose)"
	@echo "  db-reset       delete the Postgres volume (re-applies db/schema.sql on next run)"
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

db-reset:
	docker compose down -v

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
