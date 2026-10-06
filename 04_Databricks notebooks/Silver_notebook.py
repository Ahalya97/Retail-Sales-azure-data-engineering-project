# Databricks notebook source
# MAGIC %md
# MAGIC ### Step1:Create silver catalog
# MAGIC Create three schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS silver_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG silver_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  SCHEMA silver_catalog.sales;
# MAGIC CREATE SCHEMA silver_catalog.marketing;
# MAGIC CREATE SCHEMA silver_catalog.web_api;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC # **** 1. Customer Data

# COMMAND ----------

##1.Read  Customer Data
df_customers=spark.table("bronze_catalog.sales.customers")
df_customers.display()


# COMMAND ----------

from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import col,when

#Drop middle name column
df_customers=df_customers.drop('middle_name')

#Combine first and last name column
df_customers=df_customers.withColumn("Full_name",concat_ws(" ",col("first_name"),col("last_name")))

#Add age group
df_customers=df_customers.withColumn("age_group"
            ,when(col("age")<25,"Young")
            .when(col("age")<50,"Adult")
            .otherwise("Senior")
)
df_customers.display()
#5. type casting
#SQL
#Pyspark
#data frame transformation + Delta tables

from pyspark.sql.functions import col,cast
from pyspark.sql.types import IntegerType, StringType, DateType
df_customers = df_customers.select(
    col("customer_id").cast("int"),
    col("first_name").cast("string"),
    col("last_name").cast("string"),
    col("gender").cast("string"),
    col("age").cast("int"),
    col("city").cast("string"),
    col("state").cast("string"),
    col("signup_date").cast("date"),
    col("inserted_date").cast("date"),
    col("lastmodified_date").cast("date"),
    col("Full_name").cast("string"),
    col("age_group").cast("string")
    )
df_customers.printSchema()   
    
#6. write the data to delta table
df_customers.write.format("delta")  \
.mode("overwrite") \
.option("overwriteSchema","true") \
.saveAsTable("silver_catalog.sales.customers")




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_catalog.sales.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Sales Data

# COMMAND ----------

#1.Reading sales transaction table data
df_sales_transactions=spark.table("bronze_catalog.sales.sales_transactions")

from pyspark.sql.functions import col, year, month, dayofmonth, quarter
df_sales_transactions = df_sales_transactions \
    .withColumn("total_amount",
                (col("quantity") * col("unit_price")) - col("discount")) \
    .withColumn("profit", ((col("quantity") * col("unit_price")) - col("discount")) * 0.2) \
    .withColumn("year", year(col("transaction_date"))) \
    .withColumn("month", month(col("transaction_date"))) 

# 3. Correct the Data types
df_sales_transactions = df_sales_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("unit_price").cast("double"),
    col("discount").cast("double"),
    col("transaction_date").cast("date"),
    col("payment_method").cast("string"),
    col("last_updated").cast("date"),
    col("total_amount").cast("double"),
    col("profit").cast("double"),
    col("year").cast("int"),
    col("month").cast("int")
)
df_sales_transactions.display()
#4. write sales transaction data to delta table
df_sales_transactions.write.mode("overwrite").saveAsTable("silver_catalog.sales.sales_transactions")



# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. categories table

# COMMAND ----------

from pyspark.sql.functions import col

# 3. categories

#Read the categories table
df_categories=spark.table("bronze_catalog.sales.categories")
df_categories.display()

# 2. Drop duplicates and convert column names to lowercase
df_categories = df_categories.toDF(*[c.lower() for c in df_categories.columns]).dropDuplicates()

# 3.Correct the Data types
df_categories = df_categories.select(
    col("category_id").cast("int"),
    col("category_name").cast("string"),
    col("inserted_date").cast("date"),
    col("lastmodified_date").cast("date")
)
# 4. Write to Silver TABLE
df_categories.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_catalog.sales.categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. stores table

# COMMAND ----------

from pyspark.sql.functions import col

# 4- stores
# 1. Read categories table
df_stores = spark.table("bronze_catalog.sales.stores")
df_stores.display()

# 2. Correct the Data types
df_stores = df_stores.select(
    col("store_id").cast("int"),
    col("store_name").cast("string"),
    col("city").cast("string"),
    col("state").cast("string"),
    col("inserted_date").cast("date"),
    col("lastmodified_date").cast("date")
)

# 3. Write to Silver TABLE
df_stores.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_catalog.sales.stores")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. products table

# COMMAND ----------

from pyspark.sql.functions import col
# 5. products table

# 1. Read products data
df_products = spark.table("bronze_catalog.sales.products")
df_products.display()

# 2. Correct the Data types
df_products = df_products.select(
    col("product_id").cast("int"),
    col("product_name").cast("string"),
    col("category_id").cast("int"),
    col("brand").cast("string"),
    col("price").cast("double"),
    col("inserted_date").cast("date"),
    col("lastmodified_date").cast("date")
)
# 6. Write to silver layer
df_products.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_catalog.sales.products")


# COMMAND ----------

# MAGIC %md
# MAGIC # 2. **Marketing data**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. campaigns table

# COMMAND ----------

from pyspark.sql.functions import col, datediff
# 1. Read campaigns data
df_campaigns = spark.table("bronze_catalog.marketing.campaigns")
df_campaigns.display()

# 2. find campaigns duration
df_campaigns = df_campaigns.withColumn("campaign_duration", datediff(col("end_date"), col("start_date")))

# 3. Correct the Data types
df_campaigns = df_campaigns.select(
    col("campaign_id").cast("int"),
    col("campaign_name").cast("string"),
    col("start_date").cast("date"),
    col("end_date").cast("date"),
    col("budget").cast("double"),
    col("inserted_date").cast("date"),
    col("lastmodified_date").cast("date"),
    col("campaign_duration").cast("int")
)

# 4. Write to silver layer
df_campaigns.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_catalog.marketing.campaigns")



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. promotions table

# COMMAND ----------

from pyspark.sql.functions import col, datediff
# 2. promotions
#1.Reading promotion data 
df_promotions=spark.table("bronze_catalog.marketing.promotions")

#2. find data where discount percentage is > 8
df_promotions = df_promotions.filter(col("discount_percentage") >= 8)


# 3. Correct the Data types
df_promotions = df_promotions.select(
    col("promotion_id").cast("int"),
    col("product_id").cast("int"),
    col("campaign_id").cast("int"),
    col("discount_percentage").cast("double"),
    col("start_date").cast("date"),
    col("end_date").cast("date"),
    col("inserted_date").cast("date"),
    col("lastmodified_date").cast("date")
)

# 6. Write to silver layer
df_promotions.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_catalog.marketing.promotions")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_catalog.marketing.promotions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. customer_campaign_map table

# COMMAND ----------

from pyspark.sql.functions import col
# 3. customer_campaign_map

# 1. Read customer_campaign_map data
df_customer_campaign_map=spark.table("bronze_catalog.marketing.customer_campaign_map")
df_customer_campaign_map.display() 
df_customer_campaign_map.printSchema()

# 2. Correct the Data types
df_customer_campaign_map = df_customer_campaign_map.select(
    col("customer_id").cast("int"),
    col("campaign_id").cast("string"),
    col("segment_id").cast("int"),
    col("response").cast("string"),
    col("response_date").cast("date"),
    col("inserted_date").cast("date"),
    col("lastmodified_date").cast("date"))
df_customer_campaign_map.printSchema()    
 

# 6. Write to silver layer
df_customer_campaign_map.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_catalog.marketing.customer_campaign_map") 
   
      


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_catalog.marketing.customer_campaign_map

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Read customer_segments table

# COMMAND ----------

# 1. Read customer_segments data
df_customer_segments=spark.table("bronze_catalog.marketing.customer_segments")
df_customer_segments.display() 
df_customer_segments.printSchema()

# 2. Correct the Data types
df_customer_segments = df_customer_segments.select(
    col("segment_id").cast("int"),
    col("segment_name").cast("string"),
    col("inserted_date").cast("date"),
    col("lastmodified_date").cast("date")
)
df_customer_segments.printSchema()

#3. Write to silver layer
df_customer_segments.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_catalog.marketing.customer_segments")




# COMMAND ----------

# MAGIC %md
# MAGIC # **3. web_api**

# COMMAND ----------

# MAGIC %md
# MAGIC ### ##### 1. carts table

# COMMAND ----------

from pyspark.sql.functions import col, round

#Reading carts data
df_carts=spark.table("bronze_catalog.web_api.carts")

# 2. correct column names
df_carts = df_carts.selectExpr(
    "`carts'][0]['id` AS cart_id",
    "`carts'][0]['total` AS total",
    "`carts'][0]['discountedTotal` AS discounted_total",
    "userId AS user_id",
    "totalQuantity AS total_quantity"
)

# 3. find avg_price_per_item 
df_carts = df_carts.withColumn("avg_price_per_item", round(col("discounted_total") / col("total_quantity"), 2))


# 4. Correct the Data types
df_carts = df_carts.select(
    col("cart_id").cast("int"),
    col("total").cast("double"),
    col("discounted_total").cast("double"),
    col("user_id").cast("int"),
    col("total_quantity").cast("int"),
    col("avg_price_per_item").cast("double")
)


#5 Writing to delta table
df_carts.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_catalog.web_api.carts")    



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. products table

# COMMAND ----------

from pyspark.sql.functions import when, col
# 1. Read products data
df_products=spark.table("bronze_catalog.web_api.products")

df_products=df_products.selectExpr(
    "id As product_id",
    "title As Product_name",
    "category",
    "price",
    "discountPercentage As discount_percentage",
    "stock",
    "brand"
)
# 3. add 100 in product_id column
df_products = df_products.withColumn("product_id", col("product_id") + 100)

# 4. Replace null with unknown in brand column
display(df_products.withColumn("brand", when(col("brand").isNull(), "unknown").otherwise(col("brand"))))

# 5. Correct the Data types
df_products = df_products.select(
    col("product_id").cast("int"),
    col("product_name").cast("string"),
    col("category").cast("string"),
    col("price").cast("double"),
    col("discount_percentage").cast("double"),
    col("stock").cast("int"),
    col("brand").cast("string")
)

#6.Writing to delta table
df_products.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_catalog.web_api.products")    


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. users table

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# 1. Read users data
df_users=spark.table("bronze_catalog.web_api.users")

# 2. correct column names
df_users=df_users.selectExpr(
    "id As customer_id",
    "firstName As first_name",
    "lastName As last_name",
    "age",
    "gender",
    "email",
    "phone"
)

# 3. add 50 in customer_id column",
df_users=df_users.withColumn("customer_id", col("customer_id")+50)
df_users.display()

# 4. Remove space, -, + from phone number
df_users = df_users.withColumn(
    "phone",
    regexp_replace(col("phone"), r"[ +\-]", "")
)

# 4. Correct the Data types
df_users = df_users.select(
    col("customer_id").cast("int"),
    col("first_name").cast("string"),
    col("last_name").cast("string"),
    col("age").cast("int"),
    col("gender").cast("string"),
    col("email").cast("string"),
    col("phone").cast("string")
)
df_users.printSchema()

# 5. Write to silver layer
df_users.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_catalog.web_api.users")
df_users.display()
