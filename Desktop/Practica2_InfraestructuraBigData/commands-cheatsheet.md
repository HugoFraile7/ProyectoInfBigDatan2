# Data Lake Commands Cheatsheet

## Docker Commands

### Start the environment
```bash
docker-compose up -d
```

### Stop the environment
```bash
docker-compose down
```

### View running containers
```bash
docker-compose ps
```

### View logs
```bash
docker-compose logs -f
# View logs for a specific service
docker-compose logs -f minio
```

### Enter container shell
```bash
docker exec -it <container_name> bash
# Examples:
docker exec -it trino bash
docker exec -it python-client bash
```

## MinIO Commands

### Using MinIO Client (mc)
```bash
# Enter MinIO client container
docker exec -it mc /bin/sh

# List buckets
mc ls minio

# List objects in a bucket
mc ls minio/raw-zone

# Copy a file to MinIO
mc cp /path/to/file minio/raw-zone/

# Download a file from MinIO
mc cp minio/raw-zone/file.csv /path/to/download/
```

## Trino SQL Commands

### Connect to Trino
```bash
docker exec -it trino trino
```

### Trino SQL Examples
```sql
-- Show all catalogs
SHOW CATALOGS;

-- Show schemas in a catalog
SHOW SCHEMAS FROM hive;

-- Show tables in a schema
SHOW TABLES FROM hive.default;

-- Get table metadata
DESCRIBE hive.default.transactions;

-- Basic query
SELECT * FROM hive.default.transactions LIMIT 10;

-- Aggregation query
SELECT 
    payment_method, 
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM 
    hive.default.transactions
GROUP BY 
    payment_method
ORDER BY 
    transaction_count DESC;

-- Join query (with dimension table)
SELECT 
    t.transaction_id,
    t.customer_id,
    t.amount,
    p.product_name,
    p.category
FROM 
    hive.default.transactions t
JOIN 
    hive.default.dim_products p ON t.product_id = p.product_id
LIMIT 20;

-- Create a new table
CREATE TABLE hive.default.monthly_summary AS
SELECT 
    year, 
    month, 
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount
FROM 
    hive.default.transactions
GROUP BY 
    year, month
ORDER BY 
    year, month;

-- Drop a table
DROP TABLE hive.default.monthly_summary;
```

## Python Client Commands

### Run scripts in the Python client
```bash
# Run ingest data script
docker exec -it python-client python /scripts/01_ingest_data.py

# Run transform data script
docker exec -it python-client python /scripts/02_process_data.py

# Run query data script
docker exec -it python-client python /scripts/05_query_data.py
```

### Interactive Python in the container
```bash
docker exec -it python-client python
```

### Example Python code to interact with MinIO
```python
from minio import Minio

# Create MinIO client
client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# List buckets
buckets = client.list_buckets()
for bucket in buckets:
    print(bucket.name)

# List objects in a bucket
objects = client.list_objects('raw-zone', recursive=True)
for obj in objects:
    print(obj.object_name)
```

### Example Python code to interact with Trino
```python
import trino
import pandas as pd

# Connect to Trino
conn = trino.dbapi.connect(
    host="trino",
    port=8080,
    user="trino",
    catalog="hive",
    schema="default",
)

# Execute a query
cursor = conn.cursor()
cursor.execute("SELECT * FROM transactions LIMIT 10")

# Get column names
columns = [desc[0] for desc in cursor.description]

# Fetch results
results = cursor.fetchall()

# Convert to DataFrame
df = pd.DataFrame(results, columns=columns)
print(df)
```
