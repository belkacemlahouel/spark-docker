# in a new window, start containers (cluster mode, 1 driver and 1 worker)
docker-compose up --build

# in a new window, start and scale:
docker-compose up --scale spark-worker=10

# in a new window, connect to the master container and run a spark shell:
docker exec -it spark-master bash
pyspark

# in a new window, submit spark job
spark-submit --master spark://spark-master:7077 /app/app.py

# spark-ui: http://localhost:8080
# spark-ui (running app): http://localhost:4040 (shutsdown fast)
# spark-history: http://localhost:18080



