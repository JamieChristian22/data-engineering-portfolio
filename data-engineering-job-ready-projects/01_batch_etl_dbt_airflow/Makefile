.PHONY: up down psql dbt-docs dbt-run dbt-test airflow-logs

up:
	docker compose up -d --build

down:
	docker compose down -v

psql:
	docker exec -it de_postgres psql -U postgres -d analytics

dbt-run:
	docker exec -it de_dbt dbt run --project-dir /usr/app/dbt

dbt-test:
	docker exec -it de_dbt dbt test --project-dir /usr/app/dbt

dbt-docs:
	docker exec -it de_dbt dbt docs generate --project-dir /usr/app/dbt
	@echo "Open dbt docs by running: docker exec -it de_dbt dbt docs serve --project-dir /usr/app/dbt --port 8081 --host 0.0.0.0"

airflow-logs:
	docker logs -f de_airflow
