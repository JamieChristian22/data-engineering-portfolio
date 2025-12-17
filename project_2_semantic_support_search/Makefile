.PHONY: help generate build_index eval api benchmark all test

help:
	@echo "make all        - generate + clean + build index + eval"
	@echo "make api        - run FastAPI (uvicorn)"
	@echo "make benchmark  - run latency benchmark"
	@echo "make test       - run unit tests"

generate:
	python -m src.generate_data

build_index:
	python -m src.build_index

eval:
	python -m src.evaluate

api:
	uvicorn src.api:app --host 0.0.0.0 --port 8000

benchmark:
	python -m src.benchmark

test:
	pytest -q

all: generate build_index eval
