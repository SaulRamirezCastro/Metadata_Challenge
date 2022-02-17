CREATE TABLE IF NOT EXISTS row_data (
	row_id  varchar(100) Not Null,
	temperature  float Not Null,
	feels_like float,
    pressure int,
	humidity int,
	dew_point float,
	uvi int,
	clouds int,
	visibility int,
	wind_speed float,
	wind_deg float,
	wind_gust float,
	row_date timestamp Not Null,
	location varchar(200) Not Null
);
CREATE TABLE IF NOT EXISTS highest_temperatures(
	row_id varchar(100) Not Null,
	temperature float Not Null,
	temperature_date timestamp Not Null,
	location varchar(200) Not Null

);
CREATE TABLE IF NOT EXISTS avg_temperatures(
	row_id varchar(100) Not Null,
	avg_temperature float,
	min_temperature float ,
	max_temperature float ,
	row_date timestamp,
	location varchar(200) Not Null
)