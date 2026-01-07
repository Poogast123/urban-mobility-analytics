import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer  # Import the Kafka library

# Configuration for simulation
ZONES = ["Centre-Ville", "Zone Industrielle", "Quartier Résidentiel", "Aéroport"]
ROAD_TYPES = ["autoroute", "avenue", "rue"]
ROAD_IDS = ["R-101", "A-205", "R-330", "A-55", "R-12"]
SENSOR_IDS = [f"SENSOR_{i}" for i in range(1, 21)]

# --- KAFKA CONFIGURATION ---
# We connect to localhost:9092 because we are running this script 
# from your machine (Host), not inside Docker.
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
TOPIC_NAME = 'traffic-events'

def generate_traffic_event():
    current_time = datetime.now()
    hour = current_time.hour
    
    sensor_id = random.choice(SENSOR_IDS)
    road_id = random.choice(ROAD_IDS)
    road_type = random.choice(ROAD_TYPES)
    zone = random.choice(ZONES)

    is_rush_hour = (8 <= hour <= 10) or (17 <= hour <= 19)
    
    if is_rush_hour:
        vehicle_count = random.randint(50, 150)
        occupancy_rate = random.randint(70, 95)
        average_speed = random.randint(5, 40)
    else:
        vehicle_count = random.randint(5, 50)
        occupancy_rate = random.randint(5, 40)
        average_speed = random.randint(40, 90)

    if road_type == "autoroute":
        average_speed += 20
    elif road_type == "rue":
        average_speed -= 10
        
    average_speed = max(0, average_speed)

    event = {
        "sensor_id": sensor_id,
        "road_id": road_id,
        "road_type": road_type,
        "zone": zone,
        "vehicle_count": vehicle_count,
        "average_speed": average_speed,
        "occupancy_rate": occupancy_rate,
        "event_time": current_time.isoformat()
    }
    
    return event

def start_generation():
    print(f"Starting Traffic Simulation. Sending to Kafka topic: {TOPIC_NAME}")
    print("Press Ctrl+C to stop.\n")
    
    try:
        while True:
            event = generate_traffic_event()
            
            # --- SEND TO KAFKA ---
            producer.send(TOPIC_NAME, value=event)
            
            # Print to console for verification
            print(f"Sent: {event['sensor_id']} | Zone: {event['zone']} | Speed: {event['average_speed']}")
            
            time.sleep(1) 
            
    except KeyboardInterrupt:
        print("\nSimulation stopped.")
        producer.close() # Close connection cleanly

if __name__ == "__main__":
    start_generation()