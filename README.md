# Real-time Air Quality Data Engineering Pipeline

## 1. Project Overview
This project is an end-to-end Real-time Data Pipeline that fetches air quality data, streams it via Kafka, processes it using Apache Spark, and stores it in Apache Cassandra. The system is fully containerized using Docker and features a "Self-Healing" mechanism to handle failures automatically.

**Workflow:**
API Source -> Airflow (Scheduler) -> Kafka (Broker) -> Spark (Processing) -> Cassandra (Storage)

![Architecture Diagram](https://res.cloudinary.com/dhohcsfbj/image/upload/v1768461300/0e65b323-1948-4c15-8929-a7603a29da97.png)

## 2. Data Source & Processing
The pipeline ingests real-time weather and air quality data from the **Open-Meteo API**.

### Data Fields
* **Raw Metrics:**
    * `Temperature`
    * `Humidity`
    * `Wind Speed`
    * `PM2.5` (Air Quality Index)
    * `Surface Pressure`
* **Computed Metrics:**
    * `Dew Point`: Calculated automatically based on *Temperature* and *Humidity* during the processing stage.

## 3. Tech Stack
* **Language:** Python 3.9
* **Orchestration:** Apache Airflow
* **Message Broker:** Apache Kafka & Zookeeper
* **Stream Processing:** Apache Spark (Structured Streaming)
* **Database:** Apache Cassandra
* **Containerization:** Docker & Docker Compose

## 4. Prerequisites
* Windows 10/11 with **WSL2** enabled.
* **Docker Desktop** installed and running.
* Minimum RAM: 8GB (Recommended 16GB).

## 5. How to Run (One-Click Setup)

### Step 1: Start Docker Desktop
Open Docker Desktop and wait until the engine status is "Running".

![Docker Desktop Running](https://res.cloudinary.com/dhohcsfbj/image/upload/v1768461367/c8e02730-6f48-43cd-a3ce-77cdac5d3a29.png)

### Step 2: Run the Automation Script
Navigate to the project folder and double-click the `run_project.bat` file.


### Step 3: Wait for Initialization
A command window will appear. It will automatically:
1. Start all Docker containers.
2. Wait for services to stabilize (approx. 45 seconds).
3. Install necessary Python dependencies (Wait for "Building wheel..." - this may take 2-5 minutes).
4. Submit the Spark Streaming job in the background.

**Do not close the window until you see the "[OK] Spark Streaming is running" message.**

![Command Window Success](https://res.cloudinary.com/dhohcsfbj/image/upload/v1768461386/1ba9c2de-1062-43ff-86da-99d89b630a89.png)

## 6. Accessing the Interfaces

Once the script finishes, you can access the services:

* **Airflow UI:** [http://localhost:8080](http://localhost:8080)
  * **Username:** `admin`
  * **Password:** `admin`
  * **Action:** Enable the DAG `user_automation` to start fetching data.
  * **Note:** The DAG is configured to trigger **every 2 minutes** by default.

![Airflow UI](https://res.cloudinary.com/dhohcsfbj/image/upload/v1768461399/f7676665-2eab-4cb1-b692-b48971670b49.png)

* **Spark Master UI:** [http://localhost:9090](http://localhost:9090)
  * View running streaming applications and workers.

## 7. Verifying Data in Cassandra

To check if data is being stored successfully, open PowerShell and run:

```bash
docker exec -it cassandra cqlsh -e "SELECT * FROM spark_stream.air_quality;"
```
## 8. Troubleshooting
- **Cassandra Exited (137/100)**: This usually means the system ran out of RAM or data corruption occurred. Run docker-compose down --volumes to reset.
- **Spark Job Not Running**: Check the logs using docker logs get-data-spark-master-1.
- **System Lag**: Ensure .wslconfig is configured to limit Docker's RAM usage to prevent Windows from freezing.
## 9. Customization
 **Changing the Data Fetch Interval**
By default, Airflow fetches data every 2 minutes. You can change this frequency by editing the DAG file (e.g., **dags/kafka_stream.py**).

Find the "schedule" parameter in the DAG definition:
```bash
with DAG(
    'user_automation',
    default_args=default_args,
    schedule='*/2 * * * *',  # <--- Change this CRON expression
    catchup=False
) as dag:
```
* **Every 5 minutes**: Change to schedule='*/5 * * * *'

* **Every hour**: Change to schedule='@hourly'
