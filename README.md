# Docker build

1. in a new window, start containers (cluster mode, 1 driver and 1 worker)
docker-compose up --build

2. in a new window, start and scale to change number of containers
docker-compose up --build --scale spark-worker=2

# Spark management

1. in a new window, connect to the master container and run a spark shell:
docker exec -it spark-master bash
pyspark

2. in a new window, submit spark job
spark-submit --master spark://spark-master:7077 /app/app.py

# Links:
- spark-ui: http://localhost:8080
- spark-ui (running app): http://localhost:4040 (shutsdown fast)
- spark-history: http://localhost:18080



