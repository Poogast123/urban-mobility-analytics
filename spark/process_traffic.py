from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Define the schema to ensure data types are correct
traffic_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("road_id", StringType(), True),
    StructField("road_type", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("average_speed", DoubleType(), True),
    StructField("occupancy_rate", IntegerType(), True),
    StructField("event_time", StringType(), True) # Read as string first
])

def run_traffic_analysis():
    # Initialize Spark Session
    # We set HDFS user to 'root' to avoid permission issues
    spark = SparkSession.builder \
        .appName("SmartCityTrafficAnalysis") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    # 1. Read Raw Data from HDFS [cite: 80]
    # Reads all JSON files from the raw directory
    input_path = "/data/raw/traffic/*/*/*.json"
    print(f"Reading data from: {input_path}")
    
    try:
        df = spark.read.schema(traffic_schema).json(input_path)
        
        # Filter out potential empty reads
        if df.count() == 0:
            print("No data found in HDFS yet!")
            return

        print(f"Loaded {df.count()} events.")

        # 2. Calculate Indicators [cite: 81]

        # A. Traffic by Zone (Average vehicle count) [cite: 83]
        traffic_by_zone = df.groupBy("zone").agg(
            avg("vehicle_count").alias("avg_traffic"),
            max("vehicle_count").alias("max_traffic")
        )

        # B. Average Speed by Road [cite: 85]
        speed_by_road = df.groupBy("road_id", "road_type").agg(
            avg("average_speed").alias("avg_speed")
        )

        # C. Congestion Detection [cite: 87]
        # We define congestion as occupancy_rate > 80%
        congested_zones = df.filter(col("occupancy_rate") > 80) \
                            .groupBy("zone") \
                            .count() \
                            .withColumnRenamed("count", "congestion_incidents") \
                            .orderBy(col("congestion_incidents").desc())

        # 3. Display Results (for verification)
        print("\n--- RAFFIC BY ZONE ---")
        traffic_by_zone.show()

        print("\n--- SPEED BY ROAD ---")
        speed_by_road.show()
        
        print("\n--- CONGESTION ALERTS ---")
        congested_zones.show()

        # 4. Save to Processed Zone (Parquet Format) [cite: 88, 95]
        # Using 'overwrite' to replace old calculations for this run
        output_path = "/data/analytics/traffic"

        print(f"Saving Analytics data to: {output_path} (Format: Parquet)")

        traffic_by_zone.write.mode("overwrite").parquet(f"{output_path}/zone_stats")
        speed_by_road.write.mode("overwrite").parquet(f"{output_path}/road_stats")
        congested_zones.write.mode("overwrite").parquet(f"{output_path}/congestion")

    except Exception as e:
        print(f"Error during processing: {e}")
        # Print stack trace for debugging
        import traceback
        traceback.print_exc()

    spark.stop()

if __name__ == "__main__":
    run_traffic_analysis()