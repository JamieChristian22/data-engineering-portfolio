.PHONY: venv install run

venv:
	python3 -m venv .venv

install:
	. .venv/bin/activate && pip install -r requirements.txt

run:
	. .venv/bin/activate && python run_quality_checks.py
