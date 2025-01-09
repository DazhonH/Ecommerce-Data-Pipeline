# End-to-End Data Pipeline: Azure Blob Storage to PostgreSQL with Jupyter Notebook


## Project Overview
This project demonstrates an end-to-end data pipeline that extracts e-commerce data from Azure Blob Storage, processes the data using Python in Jupyter Notebook, and loads it into a PostgreSQL database. The pipeline is designed to facilitate efficient data storage, processing, and analysis.

## Architecture Diagram
![ETL PIpeline](https://github.com/user-attachments/assets/f7b01f40-cfc1-4ee8-be97-b92089912566)

## Architecture
1. **Data Extraction**
- Source: Azure Blob Storage
- Method: Azure SDK for Python - [Azure Blob Storage](https://github.com/Azure/azure-sdk-for-python/tree/azure-storage-blob_12.24.0/sdk/storage/azure-storage-blob/)
- First, you must install the library package:
``` bash
pip install azure-storage-blob
```
-After that was installed, I imported the necessary libraries:
```python
#Importing Libraries
from azure.storage.blob import BlobServiceClient
import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2
```
- To download the data from the azure blob, I used this script:
```python
# Azure Blob Storage connection details
connection_string = "Your_Connection_String_Here"
container_name = "Your_Container_Name_Here" 
local_download_path = "Your_Local_Path_Here" 

# Ensure the local directory exists
os.makedirs(local_download_path, exist_ok=True)

# Initialize BlobServiceClient and container client
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# List of CSV file names to download
csv_files = ['aisles.csv','departments.csv','order_products.csv','orders.csv','products.csv']

# Download each CSV file
for file_name in csv_files:
    blob_client = container_client.get_blob_client(file_name)
    download_file_path = os.path.join(local_download_path, file_name)
    
    print(f"Downloading {file_name} to {download_file_path}...")
    with open(download_file_path, "wb") as file:
        file.write(blob_client.download_blob().readall())
    print(f"Downloaded: {file_name}")

print("All files have been downloaded!")
```

2. **Data Processing**
- Tool: Jupyter Notebook
- Operations: Data Transformation
- The CSV files were downloaded and read into the notebook, followed by a few transformations. The order products and orders CSV contain over a million rows. So I only read a sample of the data with a random state = 42 producing the same sample everytime I run the code.
```python
#Reading The CSV Files
aisles_df = pd.read_csv("/Users/dazhonhunt/Downloads/ETL Project/aisles.csv")
departments_df = pd.read_csv("/Users/dazhonhunt/Downloads/ETL Project/departments.csv")
order_products_df = pd.read_csv("/Users/dazhonhunt/Downloads/ETL Project/order_products.csv")
orders_df = pd.read_csv("/Users/dazhonhunt/Downloads/ETL Project/orders.csv")
products_df = pd.read_csv("/Users/dazhonhunt/Downloads/ETL Project/products.csv")
```
```python
# Sample the orders table (e.g., 10,000 rows)
sampled_orders_df = orders_df.sample(10000, random_state=42)

# Sample the order_products table based on the sampled order_id values
sampled_order_ids = sampled_orders_df['order_id'].unique()
sampled_order_products_df = order_products_df[order_products_df['order_id'].isin(sampled_order_ids)]

sampled_orders_df.drop('eval_set', inplace=True,axis=1)
```

3. **Data Loading**
- Destination: PostgreSQL
- Method: SQLAlchemy, Psycopg2
- First I connected to PostgreSql
```python
#Connection to PostgreSQL
try:
    # Attempt to Connect to the Database
    conn = psycopg2.connect(
        dbname="Your_DB_Name",
        user="Your_User_Name",
        password="Your_Password",
        port="Port"
    )
    print("Connection successful")
except Exception as e:
    # Handle Connection Failure
    print("Connection unsuccessful")
    print(f"Error: {e}")
```
Then created the engine:
```python
try:
    #Creating Engine
    engine = create_engine("postgresql+psycopg2://postgres:Password@localhost/database_name")
    print("Engine Successfully Created")

except Exception as e:
    # Handle Connection Failure
    print("Unsuccesful")
    print(f"Error: {e}")
 ```

Before Loading to PostgreSql I created the Data model then created the script for the tables.

## Data Model
This project uses the following database schema.
![Ecom Data Model](https://github.com/user-attachments/assets/edb9c9b9-5d47-45c9-a153-7a346d9cd98d)

## Tables
1. **aisles**
```python
cur.execute("""
CREATE TABLE IF NOT EXISTS aisles(
    aisle_id INTEGER PRIMARY KEY,
    aisle VARCHAR(255)
    )
""")
```
2. **departments**
```python
cur.execute("""
CREATE TABLE IF NOT EXISTS departments(
    department_id INTEGER PRIMARY KEY,
    department VARCHAR(255)
    )
""")
```
3. **products**
```python
cur.execute("""
CREATE TABLE IF NOT EXISTS products(
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    aisle_id INTEGER,
    department_id INTEGER,
    FOREIGN KEY (aisle_id) REFERENCES aisles(aisle_id),
    FOREIGN KEY (department_id) REFERENCES departments (department_id)
    )
""")
```
4. **orders**
```python
cur.execute("""
CREATE TABLE IF NOT EXISTS orders(
    order_id INTEGER PRIMARY KEY,
    user_id INTEGER,
    order_number INTEGER,
    order_dow INTEGER,
    order_hour_of_day INTEGER,
    days_since_prior_order INTEGER
    )
""")
```
5. **order_products**
```python
cur.execute("""
CREATE TABLE IF NOT EXISTS order_products(
    order_id INTEGER,
    product_id INTEGER,
    add_to_cart_order INTEGER,
    reordered INTEGER,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders (order_id), 
    FOREIGN KEY (product_id) REFERENCES products (product_id)
    )
""")
```
Tables are created in pgAdmin 4

<img width="150" alt="Screenshot 2025-01-08 at 10 12 20 PM" src="https://github.com/user-attachments/assets/a3d17c4f-eff1-48ba-bfbe-d73f1cc8ea2b" />
   <br><br>

## **Loading Dataframes to PostgreSQL**

```python
aisles_df.to_sql('aisles',con=engine, if_exists= 'append', index=False)
departments_df.to_sql('departments',con=engine, if_exists = 'append', index=False)
sampled_order_products_df.to_sql('order_products', con=engine, if_exists = 'append', index=False)
sampled_orders_df.to_sql('orders',con=engine, if_exists='append', index=False)
products_df.to_sql('products', con=engine, if_exists='append', index=False)
```

## **Analysis Questions**
This project answers the following key business questions
1. 
