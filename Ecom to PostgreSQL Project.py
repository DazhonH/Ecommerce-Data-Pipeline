#!/usr/bin/env python
# coding: utf-8

# # Extracting From Azure Blob Storage

# In[5]:


# Downloading Azure Library Package
pip install azure-storage-blob


# In[1]:


#Importing Libraries
from azure.storage.blob import BlobServiceClient
import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2


# In[3]:


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


# # Transformation

# In[37]:


#Reading The CSV Files
aisles_df = pd.read_csv("/Users/dazhonhunt/Downloads/ETL Project/aisles.csv")
departments_df = pd.read_csv("/Users/dazhonhunt/Downloads/ETL Project/departments.csv")
order_products_df = pd.read_csv("/Users/dazhonhunt/Downloads/ETL Project/order_products.csv")
orders_df = pd.read_csv("/Users/dazhonhunt/Downloads/ETL Project/orders.csv")
products_df = pd.read_csv("/Users/dazhonhunt/Downloads/ETL Project/products.csv")


# In[38]:


# Sample the orders table (e.g., 10,000 rows)
sampled_orders_df = orders_df.sample(10000, random_state=42)

# Sample the order_products table based on the sampled order_id values
sampled_order_ids = sampled_orders_df['order_id'].unique()
sampled_order_products_df = order_products_df[order_products_df['order_id'].isin(sampled_order_ids)]



# In[69]:


sampled_orders_df.drop('eval_set', inplace=True,axis=1)


# In[70]:


sampled_orders_df


# ## Connecting to PostgreSQL Database

# In[53]:


#Connection to PostgreSQL
try:
    # Attempt to Connect to the Database
    conn = psycopg2.connect(
        dbname="ecom_project",
        user="postgres",
        password="Esmio",
        port="5432"
    )
    print("Connection successful")
except Exception as e:
    # Handle Connection Failure
    print("Connection unsuccessful")
    print(f"Error: {e}")


# In[54]:


cur = conn.cursor()


# In[55]:


try:
    #Creating Engine
    engine = create_engine("postgresql+psycopg2://postgres:Esmio@localhost/ecom_project")
    print("Engine Successfully Created")

except Exception as e:
    # Handle Connection Failure
    print("Unsuccesful")
    print(f"Error: {e}")
    


# ## Creating Tables

# In[ ]:


cur.execute("""
CREATE TABLE IF NOT EXISTS aisles(
    aisle_id INTEGER PRIMARY KEY,
    aisle VARCHAR(255)
    )
""")


# In[ ]:


cur.execute("""
CREATE TABLE IF NOT EXISTS departments(
    department_id INTEGER PRIMARY KEY,
    department VARCHAR(255)
    )
""")


# In[ ]:


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


# In[56]:


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


# In[57]:


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


# In[58]:


conn.commit()


# # Loading Data Into PostgreSQL

# In[123]:


aisles_df.to_sql('aisles',con=engine, if_exists= 'append', index=False)


# In[124]:


departments_df.to_sql('departments',con=engine, if_exists = 'append', index=False)


# In[72]:


sampled_order_products_df.to_sql('order_products', con=engine, if_exists = 'append', index=False)


# In[71]:


sampled_orders_df.to_sql('orders',con=engine, if_exists='append', index=False)


# In[6]:


products_df.to_sql('products', con=engine, if_exists='append', index=False)

