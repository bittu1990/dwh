// Databricks notebook source
// MAGIC %md
// MAGIC ##This notebook is used for performing ETL and analysing customer review dataset for various products.
// MAGIC 
// MAGIC ###The following steps are being followed:
// MAGIC 
// MAGIC    1. Import all required libraries
// MAGIC    2. Configure Snowflake connection.
// MAGIC    3. Run scripts on Snowflake and log error or success messages.
// MAGIC    4. Load dimension and fact tables.
// MAGIC    5. Perform required analysis on FACT_PRODUCT_REVIEWS.
// MAGIC 
// MAGIC ###Note:
// MAGIC Due to storage restriction in AWS trial version, the ETL process (Step 3) is performed in the Snowflake by executing SQL scripts. We will directly load the FACT table from Snowflake and perform our analysis. 

// COMMAND ----------

// DBTITLE 1,Step 1: Import all libraries
import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, from_json}

import time
import snowflake.connector

// COMMAND ----------

// DBTITLE 1,Step 2: Configuring Snowflake connection
//Databricks scope configuration is required
val user = dbutils.secrets.get("sfuser", "sfuser")
val password = dbutils.secrets.get("sfpassword", "sfpassword")

//Snowflake connection
val conn_param = Map(
                  "sfUrl" -> "https://YB63926.us-east-2.aws.snowflakecomputing.com/",
                  "sfUser" -> user,
                  "sfPassword" -> password,
                  "sfDatabase" -> "TAKEAWAY_DWH",
                  "sfSchema" -> "PUBLIC",
                  "sfWarehouse" -> "COMPUTE_WH"
                 )

// COMMAND ----------

// DBTITLE 1,Step 3: Run ELT scripts on Snowflake
// MAGIC %python
// MAGIC 
// MAGIC timestr = time.strftime("%Y%m%d-%H%M%S")
// MAGIC 
// MAGIC #Snowflake connection
// MAGIC con = snowflake.connector.connect(
// MAGIC     account="yb63926.us-east-2.aws",
// MAGIC     user="adminuser",
// MAGIC     password="dummy_pass",
// MAGIC     database="TAKEAWAY_DWH",
// MAGIC     schema="PUBLIC"
// MAGIC )
// MAGIC 
// MAGIC #Ingesting raw JSON data from S3 to Snowflake
// MAGIC ret_values = ''
// MAGIC try:
// MAGIC   with open('/dbfs/FileStore/etl_scripts/1_Ingest.sql') as f:
// MAGIC     for cur in con.execute_stream(f):
// MAGIC       for ret in cur:
// MAGIC         ret_values += str(ret)
// MAGIC         
// MAGIC except snowflake.connector.errors.ProgrammingError as error_msg:
// MAGIC   f = open("/dbfs/FileStore/logs/" + timestr + "_1_Ingest_error.log" , "w")
// MAGIC   f.write(str(error_msg))
// MAGIC   f.close()
// MAGIC 
// MAGIC else:
// MAGIC   f = open("/dbfs/FileStore/logs/" + timestr + "_1_Ingest_success.log" , "w")
// MAGIC   f.write(ret_values)
// MAGIC   f.close()
// MAGIC 
// MAGIC   
// MAGIC #Extracting JSON data into Staging tables
// MAGIC ret_values = ''  
// MAGIC try:
// MAGIC   with open('/dbfs/FileStore/etl_scripts/2_Extract.sql') as f:
// MAGIC     for cur in con.execute_stream(f):
// MAGIC       for return_msg in cur:
// MAGIC         for ret in cur:
// MAGIC           ret_values += str(ret)
// MAGIC         
// MAGIC except snowflake.connector.errors.ProgrammingError as error_msg:
// MAGIC   f = open("/dbfs/FileStore/logs/" + timestr + "_2_Extract_error.log" , "w")
// MAGIC   f.write(str(error_msg))
// MAGIC   f.close()
// MAGIC 
// MAGIC else:
// MAGIC   f = open("/dbfs/FileStore/logs/" + timestr + "_2_Extract_success.log" , "w")
// MAGIC   f.write(ret_values)
// MAGIC   f.close()
// MAGIC 
// MAGIC   
// MAGIC #Transforming and loading Staging data into Dimensions and Fact
// MAGIC ret_values = ''  
// MAGIC try:
// MAGIC   with open('/dbfs/FileStore/etl_scripts/3_Load.sql') as f:
// MAGIC     for cur in con.execute_stream(f):
// MAGIC       for return_msg in cur:
// MAGIC         for ret in cur:
// MAGIC           ret_values += str(ret)
// MAGIC         
// MAGIC except snowflake.connector.errors.ProgrammingError as error_msg:
// MAGIC   f = open("/dbfs/FileStore/logs/" + timestr + "_3_Load_error.log" , "w")
// MAGIC   f.write(str(error_msg))
// MAGIC   f.close()
// MAGIC 
// MAGIC else:
// MAGIC   f = open("/dbfs/FileStore/logs/" + timestr + "_3_Load_success.log" , "w")
// MAGIC   f.write(ret_values)
// MAGIC   f.close()
// MAGIC 
// MAGIC con.close()

// COMMAND ----------

// DBTITLE 1,Step 4: Load all dimension and fact tables
//reading product dimension only active rows
val df_product = spark.read
  .format("snowflake")
  .options(conn_param)
  .option("query", "select * from DIM_PRODUCT where active_flag = 1")
  .load()

//reading product category dimension
val df_product_category = spark.read
  .format("snowflake")
  .options(conn_param)
  .option("dbtable", "DIM_PROD_CATEGORY")
  .load()

//reading price bucket dimension
val df_price_bucket = spark.read
  .format("snowflake")
  .options(conn_param)
  .option("dbtable", "DIM_PRICE_BUCKET")
  .load()

//readingproduct reviews fact
val df_product_reviews = spark.read
  .format("snowflake")
  .options(conn_param)
  .option("query", "select * from FACT_PRODUCT_REVIEWS where created_date = sysdate()")
  .load()

//creating views for all dataframe
df_product.createOrReplaceTempView("vw_product")
df_product_category.createOrReplaceTempView("vw_prod_category")
df_price_bucket.createOrReplaceTempView("vw_price_bucket")
df_product_reviews.createOrReplaceTempView("vw_fct_product_review")

// COMMAND ----------

// MAGIC %md
// MAGIC ##Step 5: Data Analysis

// COMMAND ----------

// DBTITLE 1,Analysis 1: Most preferred product brands  (Top 10)
// MAGIC %sql
// MAGIC select Product_Category, Product_Brand, Total_Reviews, Highest_Rated_Reviews, Lowest_Rated_Reviews,
// MAGIC round(Highest_Rated_Reviews / coalesce(Lowest_Rated_Reviews,0,1),2) as Preference_Ratio from (
// MAGIC select pc.category Product_Category, p.brand Product_Brand, count(pr.review_id) Total_Reviews, count( case pr.rating when 5 then 5 end ) Highest_Rated_Reviews, count( case pr.rating when 1 then 1 end ) Lowest_Rated_Reviews from vw_fct_product_review pr inner join vw_product p on pr.product_id = p.product_id inner join vw_prod_category pc on pr.category_id = pc.category_id group by pc.category,p.brand ) order by  Total_Reviews desc, Preference_Ratio limit 10;

// COMMAND ----------

// DBTITLE 1,Analysis 2: Most reviewed product brand per category (Top 10)
// MAGIC %sql
// MAGIC select Product_Category, Product_Brand, Total_Reviews from (select Product_Category, Product_Brand, Total_Reviews, rank() OVER (partition by Product_Category order by Total_Reviews desc) as rank from ( select pc.category Product_Category, p.brand Product_Brand, count(pr.review_id) Total_Reviews from vw_fct_product_review pr inner join vw_product p on pr.product_id = p.product_id inner join vw_prod_category pc on pr.category_id = pc.category_id group by pc.category,p.brand ) ) where rank = 1 order by Total_Reviews desc limit 10;

// COMMAND ----------

// DBTITLE 1,Analysis 3: Most reviewed product categories (Top 10)
// MAGIC %sql
// MAGIC select pc.category Product_Category, count(pr.review_id) Total_Reviews from vw_fct_product_review pr inner join vw_prod_category pc
// MAGIC on pr.category_id = pc.category_id group by  pc.category order by count(pr.review_id) desc limit 10;

// COMMAND ----------

// DBTITLE 1,Analysis 4: Price Bucket vs Product Review Count
// MAGIC %sql
// MAGIC select pb.price_bucket_name Price_Bucket, count(pr.product_id) Total_Products_Reviewed from
// MAGIC vw_fct_product_review pr inner join vw_price_bucket pb
// MAGIC on pr.price_bucket_id = pb.price_bucket_id 
// MAGIC group by pb.price_bucket_name;
