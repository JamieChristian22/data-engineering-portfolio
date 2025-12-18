.PHONY: help generate baseline run test

help:
	@echo "make baseline - generate baseline dataset + baseline report"
	@echo "make run      - run monitor against latest data"
	@echo "make test     - run unit tests"

generate:
	python -m src.generate_data

baseline: generate
	python -m src.monitor --mode baseline

run: generate
	python -m src.monitor --mode run

test:
	pytest -q
