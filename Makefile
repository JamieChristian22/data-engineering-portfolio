.PHONY: help test all p1 p2 p3

help:
	@echo "Targets:"
	@echo "  make test   - run unit tests for all projects"
	@echo "  make p1     - run project 1 end-to-end"
	@echo "  make p2     - build index + run evaluation"
	@echo "  make p3     - run monitor + generate report"
	@echo "  make all    - run p1 + p2 + p3"

test:
	cd project_1_event_enrichment && pytest -q
	cd project_2_semantic_support_search && pytest -q
	cd project_3_data_reliability_monitor && pytest -q

p1:
	cd project_1_event_enrichment && make all

p2:
	cd project_2_semantic_support_search && make all

p3:
	cd project_3_data_reliability_monitor && make run

all: p1 p2 p3
