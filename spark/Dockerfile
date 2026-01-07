FROM apache/spark:3.4.2-python3

# Set working directory inside the container
WORKDIR /app

# Switch to root to copy files (permissions safety)
USER root
COPY process_traffic.py /app/process_traffic.py

# Switch back to the default Spark user
USER 185

# The command to run when the container starts
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--name", "TrafficAnalysis", \
     "/app/process_traffic.py"]