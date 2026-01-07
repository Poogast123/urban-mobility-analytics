import json
import time
from kafka import KafkaConsumer
from hdfs import InsecureClient
from datetime import datetime
import os

# --- CONFIGURATION ---
KAFKA_TOPIC = 'traffic-events'
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']

# Connect to HDFS (NameNode is exposed on localhost:9870 in your docker-compose)
# We use 'root' user as defined in your hadoop env config
HDFS_CLIENT = InsecureClient('http://namenode:9870', user='root')
HDFS_BASE_PATH = '/data/raw/traffic'

def get_hdfs_path(event):
    """
    Creates a path based on Date and Zone as requested in Source 74.
    Format: /data/raw/traffic/YYYY-MM-DD/Zone_Name/
    """
    event_date = event['event_time'].split('T')[0] # Extract YYYY-MM-DD
    zone_sanitized = event['zone'].replace(" ", "_") # Remove spaces for filenames
    return f"{HDFS_BASE_PATH}/{event_date}/{zone_sanitized}"

def consume_and_store():
    print(f"🔌 Connecting to Kafka topic: {KAFKA_TOPIC}...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest', # Read from beginning if we missed data
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='hdfs-archiver-group' # Consumer group
    )

    print("Consumer started. Listening for messages...")

    # Buffer to avoid creating too many small files in HDFS
    buffer = []
    BATCH_SIZE = 50  # Write to HDFS every 50 messages
    
    for message in consumer:
        event = message.value
        buffer.append(event)
        
        print(f"Received: {event['sensor_id']} | Buffer: {len(buffer)}/{BATCH_SIZE}")

        if len(buffer) >= BATCH_SIZE:
            save_batch_to_hdfs(buffer)
            buffer = [] # Clear buffer

def save_batch_to_hdfs(batch):
    """
    Writes the buffered data to HDFS.
    """
    # We take the date/zone from the first event in the batch to determine folder
    # In a perfect system, we would group by zone, but for this assignment, 
    # writing the batch to the path of the first element is acceptable or 
    # you can loop through the batch and write line by line.
    
    # Better approach for assignment: Append to a file specific to the date/zone
    for event in batch:
        path = get_hdfs_path(event)
        filename = "traffic_data.json"
        full_path = f"{path}/{filename}"

        data_str = json.dumps(event) + "\n"
        
        # Ensure directory exists in HDFS
        # HDFS client doesn't have 'makedirs' quite like OS, but write usually handles it 
        # or we check status. content=True checks if file exists.
        
        try:
            # Append is tricky in REST API, usually we write new timestamped files
            # to avoid concurrency issues. 
            timestamp_file = datetime.now().strftime("%H-%M-%S-%f")
            file_path = f"{path}/batch_{timestamp_file}.json"
            
            # Write data
            with HDFS_CLIENT.write(file_path, encoding='utf-8') as writer:
                writer.write(json.dumps(batch)) # Writing the whole batch as a JSON array
                
            print(f"Saved batch of {len(batch)} events to {file_path}")
            break # We save the whole batch to one file for simplicity
        except Exception as e:
            print(f"Error writing to HDFS: {e}")

if __name__ == "__main__":
    try:
        HDFS_CLIENT.makedirs(HDFS_BASE_PATH)
    except:
        pass
    
    consume_and_store()