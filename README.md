# ETL and analysing Amazon product reviews dataset
    1.  The following steps are being followed:
    2.  Import all required libraries
    3.  Configure Snowflake connection.
    4.  Run scripts on Snowflake and log error or success messages.
    5.  Load dimension and fact tables.
    5.  Perform required analysis on FACT_PRODUCT_REVIEWS.

# Note:
Due to storage restriction in AWS trial version, the ETL process (Step 3) is performed in the Snowflake by executing SQL scripts. We will directly load the FACT table from Snowflake and perform our analysis.

## Setup details:

-	AWS S3 bucket to store the raw compressed data. (Low Cost)
-	Snowflake free trial version to process compressed JSON data. (Could have been done on Redshift but not available for free)
-	Databricks free trial version for Spark cluster and quick visualization. (AWS EMR and QuickSight not available for free)
-	Screenshots attached


## ETL Codes and Scripts:

-	"DDL.sql" to create all required tables in Snowflake.
-	"1_Ingest.sql" to load JSON data into the raw tables.
-	"2_Extract.sql" will parse the JSON data and load into staging tables.
-	"3_Load.sql" will tranform the data from staging tables and populate all dimension and fact tables.
-	"Takeaway_ETL_Data_Analysis.scala" is the source code for the notebook.
-	"etl_process.py" is the python script to do ETL flow. (It can be used for scheduling or automating outside Databricks)


## Databricks Notebook:

-	Entire process flow is written in the notebook to perform the ETL and Analysis.
-	Supports multi language (Scala, Python, SparkQL) on Spark cluster.
-	Visualization is also done within the notebook.
- 	The notebook is scheduled in the Databricks platform.
-	It can be accessed or imported from the attached "Takeaway_ETL_Data_Analysis.html" file or below URL:

	https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2269326428961072/3244486160382768/7487126396143688/latest.html
