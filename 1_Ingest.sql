--Extracting metadata JSON from s3 bucket
COPY INTO raw_metadata FROM @TAKEAWAYRAWDATA/metadata.json.gz FILE_FORMAT = json_file_format;

--Extracting reviews JSON from s3 bucket
COPY INTO raw_reviews FROM @TAKEAWAYRAWDATA/item_dedup.json.gz FILE_FORMAT = json_file_format;