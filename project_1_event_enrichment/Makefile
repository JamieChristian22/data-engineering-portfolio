.PHONY: run test clean

run:
	python -m src.generate_data --n_users 1500 --days 14
	python -m src.train_model
	python -m src.enrich_stream --batch_size 5000
	python -m src.warehouse --db_path reports/warehouse.sqlite
	python -m src.analytics --db_path reports/warehouse.sqlite

test:
	python -m unittest discover -s tests -p "test_*.py"

clean:
	rm -rf data/processed/* models/* reports/*
