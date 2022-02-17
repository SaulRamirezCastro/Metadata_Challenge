CREATE TABLE IF NOT EXISTS row_data (
	row_id  varchar(100) Not Null PRIMARY key ,
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
	row_id varchar(100) Not Null PRIMARY KEY ,
	temperature float Not Null,
	temperature_date timestamp Not Null,
	location varchar(200) Not Null

);
CREATE TABLE IF NOT EXISTS avg_temperatures(
	row_date date Not Null PRIMARY KEY ,
	avg_temp float,
	min_temp float ,
	max_temp float ,
	min_location varchar(200) Not Null,
	max_location varchar(200) Not Null
)