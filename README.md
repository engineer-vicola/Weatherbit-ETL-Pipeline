# Weather ETL Pipeline


## Architecture
![Architecture Diagram](docs/weather_pipeline_architecture.png)

This project implements an incremental ETL workflow for collecting hourly weather forecasts, moving data from **Weatherbit API → MongoDB → Airbyte → MotherDuck**.
It covers the full data engineering process from ingestion to analytics-ready datasets.

# How the Pipeline Works

1. **Extract** – Pulls hourly forecast data from the Weatherbit API.

2. **Transform** – Converts JSON responses into a structured, normalized schema.

3. **Load (staging)** – Inserts or updates records in MongoDB using unique IDs and indexed collections.

4. **Incremental Sync** (Replicate) – Uses Airbyte to transfer only new or updated records to the warehouse while avoiding   duplicates.

5. **Analytics Warehouse** – Prepares query-friendly tables in MotherDuck for analysis and reporting.

# Technology Stack

**Python** – Handles API calls, normalization and MongoDB operations.

**MongoDB** – Staging database with replica sets and indexes for performance.

**Airbyte** – Performs incremental replication with primary key + cursor logic.

**MotherDuck** – Serves as the analytics-ready warehouse for fast querying.

# Key Highlights

- Supports hourly incremental updates

- Ensures data deduplication through deterministic IDs

- Provides ready-to-query datasets for analytics

- Demonstrates end-to-end pipeline design from API ingestion to warehouse


## 1) Prerequisites

- Python 3.10+  
- `pip install -r requirements.txt` (includes: `requests`, `pymongo`, `python-dotenv`) 
- MongoDB running locally
- Weatherbit API key
- Airbyte (**Cloud** or **Self-Hosted**)
- MotherDuck account + service token (for DuckDB in the cloud)

## 2) Configure MongoDB (single-node replica set)

- Airbyte’s Mongo source expects a replica set, no matter the number of We nodes.

1. Edit Mongo config:
   ```bash
   sudo nano /etc/mongod.conf
   ```

    - Ensure this is correct and they exist:

    ```bash
    net:
    bindIp: 0.0.0.0
    replication:
    replSetName: rs0
    ```

- Restart Mongo:

    sudo systemctl restart mongod
    sudo systemctl status mongod


    Initiate the replica set (set host to the IP Airbyte will reach):

    ```bash
    mongosh
    rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "127.0.0.1:27017" }] })
    rs.status()
    ```

- Create project DB + user:

    ```bash
    use mongodb_database
    db.createUser({
    user: ,
    roles: [{ role: "readWrite", db: "mongodb_database" }]
    })
    ```


- (Optional but recommended) Indexes for sync + uniqueness:

    ```bash
    db.weather.createIndex({ updatedAt: 1 })
    db.weather.createIndex({ city: 1, dt: 1 }, { unique: true })
    ```

## 3) Configure .env

Create .env

```bash
url=https://api.weatherbit.io/v2.0/forecast/hourly?city=Lagos,NG&hours=24&key=YOUR_API_KEY
username=xxxxx
password=xxxxx
database=xxxxx
```

## 4) Run the ingestion (Weatherbit → MongoDB)
```bash
python weatherbit.py

Expected output:

fetched=49, upserted/modified=49
```

## 5) Set up Airbyte

You can use Airbyte Cloud or Self-Hosted. The connection settings are the same.

### Add Source: MongoDB

Cluster type: Self-managed replica set

Connection string:

```python
mongodb://skyuser:skypass123@<HOST>:27017/skylogix?authSource=skylogix&replicaSet=rs0
```


- If Airbyte runs on the same host, use localhost.

- If Airbyte runs in Minikube, use your PC IP


### Add Destination: MotherDuck

MotherDuck:

- Paste MotherDuck API token

- Destination path (DB): md:skylogix

- Schema: public or your defined schema

- Test & Save


### Create the Connection (Mongo → MotherDuck)

Schedule: Manual for first run → then Every 15 min (or hourly)

Run Sync.

## Verify in MotherDuck

MotherDuck (DuckDB CLI):
```sql
SELECT COUNT(*) FROM public.weather;

SELECT city, dt, temp_c, precip_mm
FROM public.weather
ORDER BY dt DESC
LIMIT 10;

