from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define Schema
traffic_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("road_id", StringType(), True),
    StructField("road_type", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("average_speed", DoubleType(), True),
    StructField("occupancy_rate", IntegerType(), True),
    StructField("event_time", StringType(), True)
])

def run_traffic_analysis():
    # Initialize Spark with HDFS config
    spark = SparkSession.builder \
        .appName("SmartCityTrafficAnalysis") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    # 1. Read Raw Data (Raw Zone)
    input_path = "/data/raw/traffic/*/*/*.json"
    print(f"Reading data from: {input_path}")
    
    try:
        df = spark.read.schema(traffic_schema).json(input_path)
        
        if df.count() == 0:
            print("No data found in HDFS!")
            return

        print(f"Loaded {df.count()} events.")

        # 2. Calculate KPIs (Processing)
        
        # A. Traffic per Zone
        traffic_by_zone = df.groupBy("zone").agg(
            avg("vehicle_count").alias("avg_traffic"),
            max("vehicle_count").alias("max_traffic")
        )

        # B. Speed per Road
        speed_by_road = df.groupBy("road_id", "road_type").agg(
            avg("average_speed").alias("avg_speed")
        )

        # C. Congestion
        congested_zones = df.filter(col("occupancy_rate") > 80) \
                            .groupBy("zone") \
                            .count() \
                            .withColumnRenamed("count", "congestion_incidents") \
                            .orderBy(col("congestion_incidents").desc())

        # 3. Save to Analytics Zone (HDFS - Parquet)
        hdfs_out = "/data/analytics/traffic"
        print(f"Saving to HDFS: {hdfs_out}")
        
        traffic_by_zone.write.mode("overwrite").parquet(f"{hdfs_out}/zone_stats")
        speed_by_road.write.mode("overwrite").parquet(f"{hdfs_out}/road_stats")
        congested_zones.write.mode("overwrite").parquet(f"{hdfs_out}/congestion")

        # 4. Save to PostgreSQL (Serving Layer for Grafana)
        print("Writing data to PostgreSQL...")
        
        db_props = {
            "user": "user",
            "password": "password",
            "driver": "org.postgresql.Driver"
        }
        jdbc_url = "jdbc:postgresql://postgres:5432/smartcity"
        
        # Write tables - ALWAYS write, even if empty
        traffic_by_zone.write.jdbc(jdbc_url, "zone_stats", mode="overwrite", properties=db_props)
        speed_by_road.write.jdbc(jdbc_url, "road_stats", mode="overwrite", properties=db_props)
        
        # CHANGED: Removed the 'if count > 0' check.
        # This forces Spark to create the empty table structure in Postgres if no congestion exists.
        congested_zones.write.jdbc(jdbc_url, "congestion", mode="overwrite", properties=db_props)
            
        print("Data successfully pushed to PostgreSQL!")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    spark.stop()

if __name__ == "__main__":
    run_traffic_analysis()