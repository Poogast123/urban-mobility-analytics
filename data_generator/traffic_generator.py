import json
import random
import time
from datetime import datetime
import uuid

# Configuration for simulation
ZONES = ["Centre-Ville", "Zone Industrielle", "Quartier Résidentiel", "Aéroport"]
ROAD_TYPES = ["autoroute", "avenue", "rue"]
ROAD_IDS = ["R-101", "A-205", "R-330", "A-55", "R-12"]

# Simulate a fixed set of sensors (IoT devices usually have static IDs)
SENSOR_IDS = [f"SENSOR_{i}" for i in range(1, 21)]  # 20 sensors

def generate_traffic_event():
    current_time = datetime.now()
    hour = current_time.hour
    
    # 1. Pick a random sensor context
    sensor_id = random.choice(SENSOR_IDS)
    road_id = random.choice(ROAD_IDS)
    road_type = random.choice(ROAD_TYPES)
    zone = random.choice(ZONES)

    # 2. Simulate "Realistic" Traffic Logic
    # Rush hours: 08:00-10:00 and 17:00-19:00
    is_rush_hour = (8 <= hour <= 10) or (17 <= hour <= 19)
    
    if is_rush_hour:
        # High traffic, low speed, high occupancy
        vehicle_count = random.randint(50, 150)
        occupancy_rate = random.randint(70, 95)
        average_speed = random.randint(5, 40) # Traffic jam speeds
    else:
        # Normal/Low traffic, high speed
        vehicle_count = random.randint(5, 50)
        occupancy_rate = random.randint(5, 40)
        average_speed = random.randint(40, 90) # Free flow speeds

    # Adjust speed slightly based on road type (Autobahn is faster)
    if road_type == "autoroute":
        average_speed += 20
    elif road_type == "rue":
        average_speed -= 10
        
    # Ensure speed doesn't go below 0
    average_speed = max(0, average_speed)

    # 3. Construct the JSON Event [cite: 23-32]
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
    print("Starting Smart City Traffic Simulation...")
    print("Press Ctrl+C to stop.\n")
    
    try:
        while True:
            event = generate_traffic_event()
            
            # Print the JSON to console (later this will go to Kafka)
            print(json.dumps(event))
            
            # Simulate real-time delay (e.g., 1 event every 2 seconds)
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nSimulation stopped.")

if __name__ == "__main__":
    start_generation()

    