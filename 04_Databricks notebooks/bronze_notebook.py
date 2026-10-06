# Databricks notebook source
# MAGIC %md
# MAGIC ## # Display or read record

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@adlsstorageahalya.dfs.core.windows.net/")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@adlsstorageahalya.dfs.core.windows.net/sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Creating catalog and schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS bronze_catalog;
# MAGIC USE CATALOG bronze_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA bronze_catalog.sales;
# MAGIC CREATE SCHEMA bronze_catalog.marketing;
# MAGIC CREATE SCHEMA bronze_catalog.web_api;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.Create tables on top of Sales

# COMMAND ----------

# DBTITLE 1,Cell 8
#1. Categories
df = spark.read.format('csv') \
    .option('header', 'true') \
    .load('abfss://bronze@adlsstorageahalya.dfs.core.windows.net/sales/categories.csv')

df.write.format('delta') \
    .mode('overwrite') \
    .option("path", "abfss://bronze@adlsstorageahalya.dfs.core.windows.net/delta/sales/categories") \
    .saveAsTable("bronze_catalog.sales.categories")

#2. customer
df = spark.read.format('csv') \
    .option('header', 'true') \
    .load('abfss://bronze@adlsstorageahalya.dfs.core.windows.net/sales/customers.csv')

df.write.format('delta') \
    .mode('overwrite') \
    .option("path", "abfss://bronze@adlsstorageahalya.dfs.core.windows.net/delta/sales/customers") \
    .saveAsTable("bronze_catalog.sales.customers")

# 3. Product
df=spark.read.format('csv') \
    .option('header', 'true') \
    .load('abfss://bronze@adlsstorageahalya.dfs.core.windows.net/sales/products.csv')

df.write.format('delta') \
    .mode('overwrite') \
    .option("path", "abfss://bronze@adlsstorageahalya.dfs.core.windows.net/delta/sales/products") \
    .saveAsTable("bronze_catalog.sales.products")


#4. sales transactions
df=spark.read.format('csv') \
    .option('header', 'true') \
    .load('abfss://bronze@adlsstorageahalya.dfs.core.windows.net/sales/sales_transactions.csv')

df.write.format('delta') \
    .mode('overwrite') \
    .option("path", "abfss://bronze@adlsstorageahalya.dfs.core.windows.net/delta/sales/transactions") \
    .saveAsTable("bronze_catalog.sales.sales_transactions")    


#5. stores
df=spark.read.format('csv') \
    .option('header', 'true') \
    .load('abfss://bronze@adlsstorageahalya.dfs.core.windows.net/sales/stores.csv')

df.write.format('delta') \
    .mode('overwrite') \
    .option("path", "abfss://bronze@adlsstorageahalya.dfs.core.windows.net/delta/sales/stores") \
    .saveAsTable("bronze_catalog.sales.stores")



# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Create tables on top of Marketing

# COMMAND ----------

#1. Campaigns
df = spark.read.format('csv') \
    .option('header', 'true') \
    .load('abfss://bronze@adlsstorageahalya.dfs.core.windows.net/marketing/campaigns.csv')

df.write.format('delta') \
    .mode('overwrite') \
    .option("path", "abfss://bronze@adlsstorageahalya.dfs.core.windows.net/delta/marketing/campaigns") \
    .saveAsTable("bronze_catalog.marketing.campaigns")

#2. customer_campaign_map
df=spark.read.format('csv') \
    .option('header', 'true') \
    .load('abfss://bronze@adlsstorageahalya.dfs.core.windows.net/marketing/customer_campaign_map.csv')

df.write.format('delta') \
    .mode('overwrite') \
    .option("path", "abfss://bronze@adlsstorageahalya.dfs.core.windows.net/delta/marketing/customer_campaign_map") \
    .saveAsTable("bronze_catalog.marketing.customer_campaign_map")

#3. customer_segments
df=spark.read.format('csv') \
    .option('header', 'true') \
    .load('abfss://bronze@adlsstorageahalya.dfs.core.windows.net/marketing/customer_segments.csv')

df.write.format('delta') \
    .mode('overwrite') \
    .option("path", "abfss://bronze@adlsstorageahalya.dfs.core.windows.net/delta/marketing/customer_segments") \
    .saveAsTable("bronze_catalog.marketing.customer_segments")
df.printSchema()    

#4. promotions
df=spark.read.format('csv') \
    .option('header', 'true') \
    .load('abfss://bronze@adlsstorageahalya.dfs.core.windows.net/marketing/promotions.csv')

df.write.format('delta') \
    .mode('overwrite') \
    .option("path", "abfss://bronze@adlsstorageahalya.dfs.core.windows.net/delta/marketing/promotions") \
    .saveAsTable("bronze_catalog.marketing.promotions")    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create tables on top of web api

# COMMAND ----------

# DBTITLE 1,Cell 12
#1. Products
df=spark.read.format('csv') \
    .option('header', 'true') \
    .load('abfss://bronze@adlsstorageahalya.dfs.core.windows.net/web-api/products')

df.write.format('delta') \
    .mode('overwrite') \
    .option("path", "abfss://bronze@adlsstorageahalya.dfs.core.windows.net/delta/web-api/products") \
    .saveAsTable("bronze_catalog.web_api.products")

#2.users
df=spark.read.format('csv') \
    .option('header', 'true') \
    .load('abfss://bronze@adlsstorageahalya.dfs.core.windows.net/web-api/users')

df.write.format('delta') \
    .mode('overwrite') \
    .option("path", "abfss://bronze@adlsstorageahalya.dfs.core.windows.net/delta/web-api/users") \
    .saveAsTable("bronze_catalog.web_api.users") 

#3.carts
df=spark.read.format('csv') \
    .option('header', 'true') \
    .load('abfss://bronze@adlsstorageahalya.dfs.core.windows.net/web-api/carts')

df.write.format('delta') \
    .mode('overwrite') \
    .option("path", "abfss://bronze@adlsstorageahalya.dfs.core.windows.net/delta/web-api/carts") \
    .saveAsTable("bronze_catalog.web_api.carts")       