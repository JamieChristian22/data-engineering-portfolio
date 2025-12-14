.PHONY: up down topic producer spark-job

up:
	docker compose up -d

down:
	docker compose down -v

topic:
	docker exec -it 02_streaming_kafka_spark_delta-kafka-1 bash -lc "kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic clickstream --partitions 3 --replication-factor 1"

producer:
	python3 spark_app/producer.py

spark-job:
	docker exec -it 02_streaming_kafka_spark_delta-spark-1 bash -lc "spark-submit --packages io.delta:delta-spark_2.12:3.2.0 /opt/spark_app/streaming_job.py"
