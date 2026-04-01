CREATE DATABASE ny_taxi;


CREATE TABLE IF NOT EXISTS ingest_tracking (
	source_name VARCHAR,
	num_rows BIGINT,
	start_time TIMESTAMP,
	end_time TIMESTAMP
);
