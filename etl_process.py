""" This script runs the ETL process in the Snowflake DWH and log all error or success messages. """

import time
import snowflake.connector

#Snowflake connection
con = snowflake.connector.connect(
    account="yb63926.us-east-2.aws",
    user="adminuser",
    password="dummy_pass",
    database="TAKEAWAY_DWH",
    schema="PUBLIC"
)

timestr = time.strftime("%Y%m%d-%H%M%S")

#Ingesting raw JSON data from S3 to Snowflake
ret_values = ''
try:
  with open('/dbfs/FileStore/etl_scripts/1_Ingest.sql') as f:
    for cur in con.execute_stream(f):
      for ret in cur:
        ret_values += str(ret)
        
except snowflake.connector.errors.ProgrammingError as error_msg:
  f = open("/dbfs/FileStore/logs/" + timestr + "_1_Ingest_error.log" , "w")
  f.write(str(error_msg))
  f.close()

else:
  f = open("/dbfs/FileStore/logs/" + timestr + "_1_Ingest_success.log" , "w")
  f.write(ret_values)
  f.close()

  
#Extracting JSON data into Staging tables
ret_values = ''  
try:
  with open('/dbfs/FileStore/etl_scripts/2_Extract.sql') as f:
    for cur in con.execute_stream(f):
      for return_msg in cur:
        for ret in cur:
          ret_values += str(ret)
        
except snowflake.connector.errors.ProgrammingError as error_msg:
  f = open("/dbfs/FileStore/logs/" + timestr + "_2_Extract_error.log" , "w")
  f.write(str(error_msg))
  f.close()

else:
  f = open("/dbfs/FileStore/logs/" + timestr + "_2_Extract_success.log" , "w")
  f.write(ret_values)
  f.close()

  
#Transforming and loading Staging data into Dimensions and Fact
ret_values = ''  
try:
  with open('/dbfs/FileStore/etl_scripts/3_Load.sql') as f:
    for cur in con.execute_stream(f):
      for return_msg in cur:
        for ret in cur:
          ret_values += str(ret)
        
except snowflake.connector.errors.ProgrammingError as error_msg:
  f = open("/dbfs/FileStore/logs/" + timestr + "_3_Load_error.log" , "w")
  f.write(str(error_msg))
  f.close()

else:
  f = open("/dbfs/FileStore/logs/" + timestr + "_3_Load_success.log" , "w")
  f.write(ret_values)
  f.close()

#Closing Snoflake connection
con.close()