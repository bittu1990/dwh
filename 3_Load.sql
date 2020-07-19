--DIM_PORDUCT Loading with SCD Type2 (Update existing records to inactive if changed)
UPDATE dim_product a SET a.price = b.price,
					     a.url = b.imUrl,
                         a.record_end_date = SYSDATE(),
                         a.active_flag = 0
					
FROM ( SELECT DISTINCT asin, price, imUrl FROM stg_metadata ) b 
WHERE a.asin = b.asin AND (a.price != b.price OR a.url != b.imUrl);

--Insert all records with active status
INSERT INTO dim_product (
							product_id,
                            asin,
							title,
                            price,
                            url,
                            brand,
                            category,
                            record_start_date,
                            record_end_date,
                            active_flag
						)
SELECT							        
        TO_NUMBER(PRODUCT_SEQ.NEXTVAL) product_id,
		asin,
        NVL(title, 'No Name') title,
        price,
        imUrl, 
        brand,
        SUBSTR(REPLACE(REPLACE(REPLACE(categories,'[[' ),']]'),'"'), 1,
		REGEXP_INSTR(REPLACE(REPLACE(REPLACE(categories,'[[' ),']]'),'"'),',')-1) category,
		SYSDATE() record_start_date,
		NULL record_end_date,
		1 active_flag

FROM stg_metadata;

		
--Load DIM_PROD_CATEGORY
TRUNCATE TABLE dim_prod_category;
INSERT INTO dim_prod_category(
								category_id,
								category
							)
SELECT 
		TO_NUMBER(PROD_CATEGORY_SEQ.NEXTVAL) category_id,
		REPLACE(x.category,']') category
FROM (
        SELECT DISTINCT SUBSTR(REPLACE(REPLACE(REPLACE(categories,'[[' ),']]'),'"'), 1, 
			   REGEXP_INSTR(REPLACE(REPLACE(REPLACE(categories,'[[' ),']]'),'"'),',')-1) category
FROM stg_metadata) x;

		
--Load DIM_PROD_CATEGORY
TRUNCATE TABLE dim_prod_category;
INSERT INTO dim_prod_category(
								category_id,
								category
							)
SELECT 
		TO_NUMBER(PROD_CATEGORY_SEQ.NEXTVAL) category_id,
		REPLACE(x.category,']') category
FROM (
        SELECT DISTINCT SUBSTR(REPLACE(REPLACE(REPLACE(categories,'[[' ),']]'),'"'), 1, 
			   REGEXP_INSTR(REPLACE(REPLACE(REPLACE(categories,'[[' ),']]'),'"'),',')-1) category
FROM stg_metadata) x;


--Load DIM_PRICE_BUCKET
TRUNCATE TABLE dim_price_bucket;
INSERT INTO dim_price_bucket(
								price_bucket_id,
								price_bucket_name,
								min_price,
								max_price
							) 

SELECT  PRICE_BUCKET_SEQ.NEXTVAL price_bucket_id,
		y.price_bucket_name,
		y.start_price,
		y.max_price
			FROM (
			  SELECT
				DISTINCT CASE 
				WHEN x.price is NULL THEN 'NA'
				WHEN x.price < 100 THEN 'Below_100'
				WHEN x.price >= 100 AND x.price < 200 THEN 'Below_200'
				WHEN x.price >= 200 AND x.price < 300 THEN 'Below_300'
				WHEN x.price >= 300 AND x.price < 400 THEN 'Below_400'
				WHEN x.price >= 400 AND x.price < 500 THEN 'Below_500'
				WHEN x.price >= 500 AND x.price < 600 THEN 'Below_600'
				WHEN x.price >= 600 AND x.price < 700 THEN 'Below_700'
				WHEN x.price >= 700 AND x.price < 800 THEN 'Below_800'
				WHEN x.price >= 800 AND x.price < 900 THEN 'Below_900'
				WHEN x.price >= 900 AND x.price < 1000 THEN 'Below_1000'
				WHEN x.price >= 1000 THEN 'Above_100'
			END AS price_bucket_name,
			
			 CASE 
				WHEN x.price is NULL THEN -1
				WHEN x.price < 100 THEN 1
				WHEN x.price >= 100 AND x.price < 200 THEN 100
				WHEN x.price >= 200 AND x.price < 300 THEN 200
				WHEN x.price >= 300 AND x.price < 400 THEN 300
				WHEN x.price >= 400 AND x.price < 500 THEN 400
				WHEN x.price >= 500 AND x.price < 600 THEN 500
				WHEN x.price >= 600 AND x.price < 700 THEN 600
				WHEN x.price >= 700 AND x.price < 800 THEN 700
				WHEN x.price >= 800 AND x.price < 900 THEN 800
				WHEN x.price >= 900 AND x.price < 1000 THEN 900
				WHEN x.price >= 1000 THEN 1000
			END AS start_price,
			
			 CASE 
				WHEN x.price is NULL THEN -1
				WHEN x.price < 100 THEN 99
				WHEN x.price >= 100 AND x.price < 200 THEN 199
				WHEN x.price >= 200 AND x.price < 300 THEN 299
				WHEN x.price >= 300 AND x.price < 400 THEN 399
				WHEN x.price >= 400 AND x.price < 500 THEN 499
				WHEN x.price >= 500 AND x.price < 600 THEN 599
				WHEN x.price >= 600 AND x.price < 700 THEN 699
				WHEN x.price >= 700 AND x.price < 800 THEN 799
				WHEN x.price >= 800 AND x.price < 900 THEN 899
				WHEN x.price >= 900 AND x.price < 1000 THEN 999
				WHEN x.price >= 1000 THEN 9999999
			END AS max_price
        
FROM (SELECT DISTINCT price FROM stg_metadata) x ) y;


--Load FACT_PRODUCT_REVIEWS
INSERT INTO fact_product_reviews(
									review_id,
									product_id,
									category_id,
									price_bucket_id,
									helpful,
									reviewtext,
									reviewtime, 
									reviewerid, 
									reviewername, 
									summary, 
									rating, 
									created_date
								)
SELECT
        REVIEW_SEQ.NEXTVAL review_id,
        dim_product.product_id,
        dim_prod_category.category_id,
        dim_price_bucket.price_bucket_id,
		TO_NUMBER(ROUND(SUBSTR(REPLACE(REPLACE(stg_reviews.helpful, '[') , ']'), 1, REGEXP_INSTR(stg_reviews.helpful, ',')-2)                                  
            / DECODE(SUBSTR(REPLACE(REPLACE(stg_reviews.helpful, '[') , ']'), REGEXP_INSTR(stg_reviews.helpful, ','),
					REGEXP_INSTR(stg_reviews.helpful, ']') ),0,1,
                    SUBSTR(REPLACE(REPLACE(stg_reviews.helpful, '[') , ']'), REGEXP_INSTR(stg_reviews.helpful, ','),
					REGEXP_INSTR(stg_reviews.helpful, ']') )),2)) AS helpful,
		
        stg_reviews.reviewtext,
        TO_DATE (REPLACE(REPLACE(stg_reviews.reviewtime,' ','.'),',',''), 'MM.DD.YYYY') AS reviewtime,
        stg_reviews.reviewerid,
        stg_reviews.reviewername,
        stg_reviews.summary,
        stg_reviews.rating,
        SYSDATE()

FROM  stg_reviews 
INNER JOIN dim_product ON stg_reviews.asin = dim_product.asin
INNER JOIN dim_prod_category ON dim_product.category = dim_prod_category.category
INNER JOIN dim_price_bucket ON dim_product.price BETWEEN dim_price_bucket.min_price AND dim_price_bucket.max_price
WHERE dim_product.category IS NOT NULL OR LENGTH(dim_product.category) > 0
OR dim_product.brand IS NOT NULL OR LENGTH(dim_product.brand) > 0;