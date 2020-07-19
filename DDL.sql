--Creating Snowflake external STAGE for s3 bucket
CREATE OR REPLACE stage TAKEAWAYRAWDATA url='s3://takeawayrawdata/'
  credentials=(aws_key_id='######' aws_secret_key='$$$$$$');
  

--Creating the File Format to accept compressed JSON
CREATE FILE FORMAT json_file_format 
TYPE = JSON
COMPRESSION = GZIP
ENABLE_OCTAL = FALSE 
ALLOW_DUPLICATE = FALSE 
STRIP_OUTER_ARRAY = TRUE 
STRIP_NULL_VALUES = FALSE 
IGNORE_UTF8_ERRORS = FALSE;


--Creating raw table for Metadata
CREATE TABLE raw_metadata (
  raw_metadata_json VARIANT
);


--Creating raw table for Reviews
CREATE TABLE raw_reviews (
  raw_reviews_json VARIANT
);

--Ceating Staging tables
CREATE TABLE stg_metadata (
							asin VARCHAR,
							title VARCHAR,
							price NUMBER(6,2),
							imurl VARCHAR,
							related VARCHAR,
							salesrank VARCHAR,
							brand VARCHAR,
							categories VARCHAR
							
						);
						
CREATE TABLE stg_reviews (
							asin VARCHAR,
							helpful VARCHAR,
							rating NUMBER(6,2),
							reviewtext VARCHAR,
							reviewtime VARCHAR,
							reviewerid VARCHAR,
							reviewername VARCHAR,
							summary VARCHAR,
							unixreviewtime VARCHAR
						);

--Creating SEQUENCE
CREATE OR REPLACE SEQUENCE PRODUCT_SEQ START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE PRICE_BUCKET_SEQ START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE PROD_CATEGORY_SEQ START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE REVIEW_SEQ START = 1 INCREMENT = 1;


--drop table dim_product
CREATE TABLE dim_product 
                        (
                          product_id NUMBER,
                          asin VARCHAR,
						  title VARCHAR,
                          price NUMBER(6,2),
                          url VARCHAR,
                          brand VARCHAR,
                          category VARCHAR,
                          record_start_date DATE,
                          record_end_date DATE,
                          active_flag NUMBER
                        );
ALTER TABLE dim_product ADD PRIMARY KEY (product_id);


--drop table dim_prod_category
CREATE TABLE dim_prod_category
                        (
                          category_id NUMBER,
                          category VARCHAR
                        );
ALTER TABLE dim_prod_category ADD PRIMARY KEY (category_id);


--drop table dim_price_bucket
CREATE TABLE dim_price_bucket
                        (
                          price_bucket_id NUMBER,
                          price_bucket_name VARCHAR,
                          min_price NUMBER(6,2),
                          max_price NUMBER(6,2)
                        );
ALTER TABLE dim_price_bucket ADD PRIMARY KEY (price_bucket_id);


--drop table fact_product_reviews
CREATE TABLE fact_product_reviews
                        (
                          review_id NUMBER,
                          product_id NUMBER,
                          category_id NUMBER,
                          price_bucket_id NUMBER,
						  helpful NUMBER,
                          reviewtext VARCHAR,
                          reviewtime DATE,
                          reviewerid VARCHAR,
                          reviewername VARCHAR,
                          summary VARCHAR,
                          rating NUMBER(6,2),
						  created_date DATE
                          );
ALTER TABLE fact_product_reviews ADD PRIMARY KEY (review_id);