FROM python:3.9-slim

# Install dependencies
RUN pip install kafka-python hdfs

# Set working directory
WORKDIR /app

# Copy the script
COPY hdfs_consumer.py .

# Run the script
CMD ["python", "-u", "hdfs_consumer.py"]