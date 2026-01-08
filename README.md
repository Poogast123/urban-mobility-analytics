<div align="center">
   <h1>Smart City Traffic Analysis Pipeline</h1>
   <p><strong>End-to-End Big Data Engineering Project</strong></p>

   <p>
      <img src="https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white" alt="Kafka" />
      <img src="https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white" alt="Spark" />
      <img src="https://img.shields.io/badge/Hadoop_HDFS-66CCFF?style=for-the-badge&logo=apache-hadoop&logoColor=black" alt="Hadoop" />
      <img src="https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white" alt="Airflow" />
      <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
      <img src="https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white" alt="Grafana" />
   </p>

   <p>
      <em>Developed by <strong>Oussama Kharifi</strong></em><br />
      <strong>Big Data & Cloud Computing Engineering – ENSET Mohammedia</strong>
   </p>
</div>

---

## Project Overview

This project implements a scalable **End-to-End Big Data Pipeline** designed to address urban mobility challenges within a **Smart City** context. By collecting, processing, and visualizing traffic data in real-time, the system provides actionable insights to detect congestion events and analyze vehicle flows across different urban zones.

### Table of Contents
- [Project Context](#-project-context)
- [Architecture](#-architecture)
- [Pipeline Stages](#-pipeline-stages)
  - [1. Data Collection & Ingestion](#1-data-collection--ingestion)
  - [2. Storage (Data Lake)](#2-storage-data-lake)
  - [3. Processing & Analytics](#3-processing--analytics)
  - [4. Visualization](#4-visualization)
  - [5. Orchestration](#5-orchestration)
- [Technology Stack](#-technology-stack)
- [Installation](#-installation)
- [Future Improvements](#-future-improvements)

---

## Project Context

Modern urban environments rely on data-driven strategies to manage complexity. This project aims to:
* Monitor traffic status in real-time.
* Detect congestion events as they occur.
* Analyze vehicle flows through different zones.
* Improve long-term urban planning.

---

## Architecture

The solution is fully containerized using **Docker** and follows an enhanced **ETL (Extract, Transform, Load)** pattern for streaming ingestion.

**High-Level Data Flow:**
1.  **Source:** Python Traffic Simulator (JSON Events).
2.  **Message Broker:** Apache Kafka (ZooKeeper + Broker).
3.  **Data Lake (Raw):** Hadoop HDFS (Namenode + Datanode).
4.  **Processing Engine:** Apache Spark (Master + Worker).
5.  **Serving Layer:** PostgreSQL (for dashboards) & HDFS (Parquet for archiving).
6.  **Visualization:** Grafana.
7.  **Orchestration:** Apache Airflow.

---

## Pipeline Stages

### 1. Data Collection & Ingestion
* **Simulation:** A Python script (`traffic_generator.py`) simulates realistic traffic patterns, including logic for "Rush Hours" (8 AM–10 AM, 5 PM–7 PM) where vehicle count and occupancy rates increase while speed decreases.
* **Data Format:** JSON events containing `sensor_id`, `road_id`, `road_type`, `vehicle_count`, `average_speed`, `occupancy_rate`, and `event_time`.
* **Ingestion:** Events are pushed to an **Apache Kafka** topic named `traffic-events` to decouple production from consumption.

### 2. Storage (Data Lake)
* **Raw Zone:** A dedicated consumer (`hdfs_consumer.py`) reads from Kafka, buffers messages to avoid "Small Files" issues, and writes raw JSON data into **HDFS**.
* **Partitioning Strategy:** Data is organized hierarchically to optimize retrieval: `/data/raw/traffic/YYYY-MM-DD/Zone/`.

### 3. Processing & Analytics
**Apache Spark** is used for batch processing and aggregation (`process_traffic.py`). The logic includes:
1.  **Schema Enforcement:** Ensuring correct data types (Integer, Double).
2.  **Aggregations:** Calculating average traffic per zone and average speed per road type.
3.  **Congestion Detection:** Identifying zones where `occupancy_rate > 80%`.
4.  **Optimization:** Results are stored in **Apache Parquet** format (Analytics Zone) for columnar compression and faster query performance.

### 4. Visualization
**Grafana** is connected to a **PostgreSQL** database (populated by Spark via JDBC) to visualize KPIs.
* **Road Speed Analysis:** Comparison of speeds across road types (e.g., Highway vs. Avenue).
* **Traffic by Zone:** Bar charts showing vehicle distribution.
* **Congestion Alerts:** Real-time panels highlighting critical zones.

### 5. Orchestration
**Apache Airflow** automates the workflow with a DAG (`smart_city_traffic_pipeline`) running every 5 minutes. The DAG tasks are:
1.  `check_kafka_ingestion`: Verifies Kafka availability.
2.  `run_spark_job`: Triggers the Spark processing container.
3.  `validate_hdfs_output`: Confirms the successful creation of analytics data in HDFS.

---

## Technology Stack

* **Ingestion:** Apache Kafka, Zookeeper.
* **Storage:** Hadoop HDFS (NameNode, DataNode).
* **Processing:** Apache Spark (Python/PySpark).
* **Database:** PostgreSQL.
* **Visualization:** Grafana.
* **Orchestration:** Apache Airflow.
* **Infrastructure:** Docker, Docker Compose.

---

## Installation

**Prerequisites:** Docker & Docker Compose.

1.  **Clone the repository**
    ```bash
    git clone [https://github.com/your-username/smart-city-pipeline.git](https://github.com/your-username/smart-city-pipeline.git)
    cd smart-city-pipeline
    ```

2.  **Start the Infrastructure**
    The project uses a comprehensive `docker-compose.yml` file to spin up Zookeeper, Kafka, Hadoop, Spark, Postgres, Grafana, and Airflow.
    ```bash
    docker-compose up -d
    ```

3.  **Start Data Generation**
    ```bash
    # Create the virtual environment and install dependencies
    python -m venv venv
    source venv/bin/activate
    pip install kafka-python hdfs

    # Run the simulator
    python data_generator/traffic_generator.py
    ```

4.  **Access Interfaces**
    * **Airflow:** `http://localhost:8080` (Trigger the DAG)
    * **Grafana:** `http://localhost:3000` (View Dashboards)
    * **HDFS:** `http://localhost:9870`

---
