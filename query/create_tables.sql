CREATE TABLE IF NOT EXISTS row_data (
	row_id  varchar(100) Not Null,
	temperature  float Not Null,
	feels_like float,
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