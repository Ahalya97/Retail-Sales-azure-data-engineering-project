# Databricks notebook source
# MAGIC %md
# MAGIC ### #### Step 1- create gold catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 create two schemas for gold_catalog
# MAGIC ### #1- gold_catalog.analytics
# MAGIC ### #2- gold_catalog.business_metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS gold_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC  create schema IF NOT EXISTS gold_catalog.analytics;
# MAGIC  create schema IF NOT EXISTS gold_catalog.business_metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog(), current_schema();

# COMMAND ----------

### ###gold_catalog.analytics
###  ├── fact_sales
###  ├── fact_campaign_response
###  ├── fact_cart_behavior

# ├── dim_customer
# ├── dim_product
# ├── dim_store
# ├── dim_date
# ├── dim_campaign
# ├── dim_segment

# COMMAND ----------

#  gold_catalog.business_metrics
# ├── sales_by_month
# ├── top_products
# ├── campaign_performance
# ├── customer_lifetime_value
# ├── conversion_rate

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3. Read all data from silver_catalog

# COMMAND ----------

# 1. Sales tables
customers = spark.table("silver_catalog.sales.customers")
sales_transactions = spark.table("silver_catalog.sales.sales_transactions")
categories = spark.table("silver_catalog.sales.categories")
stores = spark.table("silver_catalog.sales.stores")
products = spark.table("silver_catalog.sales.products")

# 2. marketing tables
campaigns = spark.table("silver_catalog.marketing.campaigns")
promotions = spark.table("silver_catalog.marketing.promotions")
customer_campaign_map = spark.table("silver_catalog.marketing.customer_campaign_map")
customer_segments = spark.table("silver_catalog.marketing.customer_segments")

# 3. web_api tables
api_carts = spark.table("silver_catalog.web_api.carts")
api_products = spark.table("silver_catalog.web_api.products")
api_users = spark.table("silver_catalog.web_api.users")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. DIM CUSTOMER

# COMMAND ----------

dim_customers=customers.join(api_users,"customer_id","left") \
    .join(customer_campaign_map,"customer_id","left") \
    .select(
        customers.customer_id,
        customers.Full_name,
        customers.gender,
        customers.age_group,
        customers.city,
        customers.state,
        api_users.email,
        customer_campaign_map.segment_id,
        customer_campaign_map.response
    ).dropDuplicates()
#Displaying data
dim_customers.display() 
# save data into gold layer
dim_customers.write.format("delta").mode("overwrite").saveAsTable("gold_catalog.analytics.dim_customers")   


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_catalog.web_api.users

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. DIM PRODUCT

# COMMAND ----------

dim_products=products.join(categories,"category_id","left") \
            .join(api_products,"product_id","left") \
            .select(
                api_products.product_id,
                api_products.product_name,
                api_products.category.alias('category_name'),
                products.brand,
                products.price,
                #api_products.products_rating,
                api_products.stock
            ).dropDuplicates()
#displaying the data            
dim_products.display()               
        
#For saving this to gold layer
dim_products.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_catalog.analytics.dim_products")            

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. DIM STORE

# COMMAND ----------

#DIM STORE

dim_stores = stores.select(
    "store_id",
    "store_name",
    "city",
    "state"
).dropDuplicates()
# display(dim_stores)

# save data into gold layer
dim_stores.write.format("delta").mode("overwrite").saveAsTable("gold_catalog.analytics.dim_stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. DIM DATE

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, quarter

# DIM DATE
dim_date= sales_transactions.select(
    sales_transactions["transaction_date"].alias("date")
).withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("quarter", quarter("date")) \
    .dropDuplicates()
#Displaying the data
dim_date.display()

#save the data into gold table
dim_date.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_catalog.analytics.dim_date")    



# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. DIM CAMPAIGN

# COMMAND ----------

# DIM CAMPAIGN
dim_campaign=campaigns.select(
    "campaign_id",
    "campaign_name",
    "start_date",
    "end_date",
    "campaign_duration"
).dropDuplicates()

#displaying the data
#dim_campaign.display()

#save this data into gold table
dim_campaign.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_catalog.analytics.dim_campaign")    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact **Tables**

# COMMAND ----------

# MAGIC %md
# MAGIC **1.**FACT** SALES (MAIN)**

# COMMAND ----------

from pyspark.sql.functions import col

# FACT SALES (MAIN)
fact_sales=sales_transactions.select(
    "transaction_id",
    "customer_id",
    "product_id",
    "store_id",
    col("transaction_date").alias("date"),
    "quantity",
    "unit_price",
    "discount",
    "total_amount",
    "profit"
)

#displaying the data
#fact_sales.display()

#Writing the data to gold layer
fact_sales.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_catalog.analytics.fact_sales")    

# COMMAND ----------

# MAGIC %md
# MAGIC **2. FACT PRODUCT PERFORMANCE**

# COMMAND ----------

from pyspark.sql.functions import sum 

#  FACT PRODUCT PERFORMANCE
fact_product_performance=sales_transactions.groupBy("product_id") \
    .agg(
        sum("total_amount").alias("total_sales"),
        sum("quantity").alias("total_quantity"),
    ).join(api_products,"product_id","left") \
    .select(
            api_products.product_id,
            api_products.product_name,
            api_products.price,
            api_products.discount_percentage,
           # "product_rating"
        )
#fact_product_performance.display()  

#Save the data to gold layer
fact_product_performance.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_catalog.analytics.fact_product_performance")    

# COMMAND ----------

# MAGIC %md
# MAGIC **3. FACT CAMPAIGN PERFORMANCE**

# COMMAND ----------

from pyspark.sql.functions import count
# FACT CAMPAIGN PERFORMANCE

fact_campaign_performance=customer_campaign_map.join(sales_transactions,"customer_id","left") \
    .groupBy("campaign_id") \
    .agg(
        count("customer_id").alias("total_customers"),
        sum("total_amount").alias("total_sales"),
        #sum("quantity").alias("total_quantity"),
    )
#fact_campaign_performance.display()  

#Save this data to gold layer
fact_campaign_performance.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_catalog.analytics.fact_campaign_performance")    

# COMMAND ----------

# MAGIC %md
# MAGIC **4. FACT CUSTOMER BEHAVIOR (CART VS SALES)**

# COMMAND ----------

from pyspark.sql.functions import sum, col, when, coalesce

# 1. Aggregate carts data at customer level
carts_agg = api_carts.groupBy("user_id").agg(
    sum("total_quantity").alias("carts_total_products"),
    sum("total").alias("carts_total")
)

# 2. Aggregate sales data at customer level
sales_agg = sales_transactions.groupBy("customer_id").agg(
    sum("quantity").alias("sales_quantity"),
    sum("total_amount").alias("total_sales_amount")
)

# 3. Join and calculate conversion rate (with division-by-zero check)
fact_customer_behavior = carts_agg.join(
    sales_agg, 
    carts_agg.user_id == sales_agg.customer_id, 
    "inner"
).withColumn(
    "conversion_rate",
    when(col("carts_total_products") > 0, col("sales_quantity") / col("carts_total_products"))
    .otherwise(0)
).drop(sales_agg.customer_id) 

# save data into gold layer
fact_customer_behavior.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_catalog.analytics.fact_customer_behavior")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Start working business level queries**

# COMMAND ----------

# MAGIC %sql
# MAGIC USE gold_catalog.business_metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load all the tables

# COMMAND ----------

# Load tables from Unity Catalog
fact_sales = spark.table("gold_catalog.analytics.fact_sales")
dim_stores = spark.table("gold_catalog.analytics.dim_stores")
dim_customers = spark.table("gold_catalog.analytics.dim_customers")
dim_products = spark.table("gold_catalog.analytics.dim_products")
fact_campaign_performance = spark.table("gold_catalog.analytics.fact_campaign_performance")
dim_campaign = spark.table("gold_catalog.analytics.dim_campaign")
fact_customer_behavior = spark.table("gold_catalog.analytics.fact_customer_behavior")

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Sales by Category**

# COMMAND ----------

# MAGIC %md
# MAGIC **Which category performs best?**

# COMMAND ----------

#Spark SQL we are using
#for entering into multiline we are using """ """
sales_by_category=spark.sql("""    
    select
        p.category_name,
        sum(f.total_amount) as category_sales
    FROM gold_catalog.analytics.fact_sales f
    JOIN gold_catalog.analytics.dim_products p
    ON f.product_id = p.product_id   
    group by p.category_name
    order by category_sales desc                     
                            """)

# Write to gold layer
sales_by_category.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_catalog.business_metrics.sales_by_category")                                

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Total Revenue by State

# COMMAND ----------

# MAGIC %md
# MAGIC  Which states generate the most revenue?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Using SQL
# MAGIC CREATE OR REPLACE TABLE gold_catalog.business_metrics.revenue_by_state AS
# MAGIC SELECT
# MAGIC ds.state,
# MAGIC sum(fs.total_amount) AS total_revenue
# MAGIC FROM gold_catalog.analytics.fact_sales fs
# MAGIC JOIN gold_catalog.analytics.dim_stores ds
# MAGIC ON fs.store_id = ds.store_id
# MAGIC group by ds.state
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Top 5 Customers by Spending

# COMMAND ----------

# MAGIC %md
# MAGIC Who are our most valuable customers?
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import sum

# Load tables from Unity Catalog
fact_sales = spark.table("gold_catalog.analytics.fact_sales")
dim_customers = spark.table("gold_catalog.analytics.dim_customers")
spending_df=fact_sales.join(dim_customers,"customer_id")
top_customers=spending_df.groupBy("customer_id","full_name") \
    .agg(sum("total_amount").alias("total_spent")) \
    .orderBy("total_spent", ascending=False) \
    .limit(5)

#top_customers.display()  

#Writing this to gold table
top_customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_catalog.business_metrics.top_customers")    

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Best Selling Products (by Quantity)

# COMMAND ----------

# MAGIC %md
# MAGIC What products sell the most?

# COMMAND ----------

from pyspark.sql.functions import sum

# Load tables from Unity Catalog
fact_sales = spark.table("gold_catalog.analytics.fact_sales")
dim_products = spark.table("gold_catalog.analytics.dim_products")
selling_products_df =fact_sales.join(dim_products,"product_id")
top_products = selling_products_df.groupBy("product_name") \
    .agg(sum("quantity").alias("total_quantity")) \
    .orderBy("total_quantity", ascending=False)

#top_products.display()

#write this to gold layer
top_products.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_catalog.business_metrics.top_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Campaign Effectiveness (Sales per Campaign)

# COMMAND ----------

# MAGIC %md
# MAGIC Which campaigns are actually driving revenue?

# COMMAND ----------

from pyspark.sql.functions import sum

# Load tables from Unity Catalog
fact_campaign_performance = spark.table("gold_catalog.analytics.fact_campaign_performance")
dim_campaign = spark.table("gold_catalog.analytics.dim_campaign")

campaign_df = fact_campaign_performance.join(dim_campaign, "campaign_id")

campaign_performance = campaign_df.select(
    "campaign_name",
    "total_customers",
    "total_sales"
).orderBy("total_sales", ascending=False)

#campaign_performance.display()
#write the data to gold layer

campaign_performance.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_catalog.business_metrics.campaign_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Conversion Rate Analysis (Cart vs Purchase)

# COMMAND ----------

# MAGIC %md
# MAGIC How many users actually convert?

# COMMAND ----------

from pyspark.sql.functions import avg, round, col

# Load table
fact_customer_behavior = spark.table("gold_catalog.analytics.fact_customer_behavior")

conversion_analysis = fact_customer_behavior.select(
    round(avg(col("conversion_rate")) * 100, 2).alias("avg_conversion_rate_pct")
)
#conversion_analysis.display()

#Write this data to gold layer
conversion_analysis.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_catalog.business_metrics.conversion_analysis")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. High Discount Impact on Sales

# COMMAND ----------

# MAGIC %md
# MAGIC Are discounts actually increasing sales?

# COMMAND ----------

from pyspark.sql.functions import sum

# Load tables from Unity Catalog
fact_sales = spark.table("gold_catalog.analytics.fact_sales")

discount_impact = fact_sales.groupBy("discount") \
    .agg(sum("total_amount").alias("total_sales")) \
    .orderBy("discount")

#discount_impact.display()   

#writing this data to gold layer
discount_impact.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_catalog.business_metrics.discount_impact")    