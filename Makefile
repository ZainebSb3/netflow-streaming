CSV      ?= data/train_test_network.csv
RATE     ?= 500
DURATION ?= 0
PY       := .venv/bin/python

.PHONY: venv up jobs replay test test-e2e bench psql logs down clean

venv:            ## create the Python environment
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt

up:              ## start Kafka, PostgreSQL and Flink
	docker compose up -d --build

jobs:            ## submit the two Flink SQL jobs (once the UI answers on :8081)
	@until curl -sf localhost:8081/overview > /dev/null; do echo "waiting for Flink..."; sleep 2; done
	docker compose exec -T jobmanager ./bin/sql-client.sh -f /opt/flink/sql/01_dedup.sql
	docker compose exec -T jobmanager ./bin/sql-client.sh -f /opt/flink/sql/02_window_agg.sql

replay:          ## replay the dataset into Kafka (RATE records/s, DURATION s; 0 = whole file)
	$(PY) -m replay.producer --csv $(CSV) --rate $(RATE) --duration-s $(DURATION)

test:            ## unit tests (no infrastructure needed)
	$(PY) -m pytest -q

test-e2e:        ## end-to-end test against the running stack
	$(PY) -m pytest -q -m e2e

bench:           ## report on the most recent replay run
	$(PY) -m bench.measure $$(ls -t bench/runs/run-*.json | head -1)

psql:            ## open a SQL shell on the results
	docker compose exec postgres psql -U flink -d netflows

logs:
	docker compose logs -f --tail=50

down:            ## stop everything and delete the checkpoint volume
	docker compose down -v

clean: down
	rm -rf bench/runs .pytest_cache
