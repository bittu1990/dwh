--Loading metadata JSON into table
TRUNCATE TABLE stg_metadata;
INSERT INTO stg_metadata (asin, title, price, imurl, related, salesrank, brand, categories )
SELECT  raw_metadata_json:asin::string AS asin,
        raw_metadata_json:title::string AS title,
        raw_metadata_json:price::NUMBER(6,2) AS price,
        raw_metadata_json:imUrl::string AS imUrl,
        raw_metadata_json:related::string AS related,
        raw_metadata_json:salesRank::string AS salesrank,
        raw_metadata_json:brand::string AS brand,
        raw_metadata_json:categories::string AS categories

FROM raw_metadata;


--Loading review JSON into table
TRUNCATE TABLE stg_reviews;
INSERT INTO stg_reviews (asin, helpful, rating, reviewtext, reviewtime, reviewerid, reviewername, summary, unixreviewtime )
SELECT  raw_reviews_json:asin::string AS asin,
        raw_reviews_json:helpful::string AS helpful,
        raw_reviews_json:overall::NUMBER(6,2) AS rating,
        raw_reviews_json:reviewText::string AS reviewtext,
        raw_reviews_json:reviewTime::string AS reviewtime,
        raw_reviews_json:reviewerID::string AS reviewerid,
        raw_reviews_json:reviewerName::string AS reviewername,
        raw_reviews_json:summary::string AS summary,
        raw_reviews_json:unixReviewTime::string AS unixreviewtime

FROM raw_reviews;