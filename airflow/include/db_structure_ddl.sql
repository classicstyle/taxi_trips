-- Stage schema
create schema if not exists stage;

CREATE table if not exists stage.taxi_trips (
	vendor_id int4 NULL,
	lpep_pickup_datetime timestamp NULL,
	lpep_dropoff_datetime timestamp NULL,
	store_and_fwd_flag varchar(1) NULL,
	ratecode_id int4 NULL,
	pulocation_id int4 NULL,
	dolocation_id int4 NULL,
	passenger_count int4 NULL,
	trip_distance numeric NULL,
	fare_amount numeric NULL,
	extra numeric NULL,
	mta_tax numeric NULL,
	tip_amount numeric NULL,
	tolls_amount numeric NULL,
	ehail_fee numeric NULL,
	improvement_surcharge numeric NULL,
	total_amount numeric NULL,
	payment_type int4 NULL,
	trip_type int4 NULL,
	congestion_surcharge numeric NULL,
	taxi_type varchar(50) NULL
);

CREATE TABLE if not exists stage.taxi_zone (
	location_id int4 NULL,
	borough varchar(255) NULL,
	"zone" varchar(255) NULL,
	service_zone varchar(255) NULL
);

-- Metrics schema

create schema if not exists metrics;

CREATE TABLE metrics.pop_dest_by_passengers (
	months varchar(10) NULL,
	pick_up varchar(255) NULL,
	drop_off varchar(255) NULL,
	passenger_count int8 NULL,
	drank int8 NULL
);

CREATE TABLE metrics.pop_dest_by_rides (
	months varchar(10) NULL,
	pick_up varchar(255) NULL,
	drop_off varchar(255) NULL,
	rides_count int8 NULL,
	drank int8 NULL
);







