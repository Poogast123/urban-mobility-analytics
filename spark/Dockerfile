FROM apache/spark:3.4.2-python3

WORKDIR /app

USER root

# 1. Install curl and download the driver
RUN apt-get update && apt-get install -y curl && \
    curl -o /opt/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

COPY process_traffic.py /app/process_traffic.py

USER 185

# 2. Use --jars to distribute the driver to workers automatically
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--name", "TrafficAnalysis", \
     "--jars", "/opt/spark/jars/postgresql-42.6.0.jar", \
     "/app/process_traffic.py"]