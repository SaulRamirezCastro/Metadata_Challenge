<h1 align="center"> Open Weather project </h1>

## Description

This personal project to get data from open weather by API and put in  PostgreSQL.
Using geopy to convert the names of the locations, like city and country, and get the latitude and longitude to make the rest api call and get 5-day forecast historical data. Use Pandas for data manipulation, get the highest and avg, min and max  temperatures by location, and persist in a relational table.


Create a Docker image for database and application and set up a docker-compose file to execute this project in your local machine.

Project Structure:
- env_config/database.env
  - config file that contains the user, password and database to setup  our local docker for PostgreSQL.
- src: main folder for the Application code
  - src/config/config.yml
    -   Yaml file with application configurations, like Rest api endpoint, secret key, database connection, locations, etc.
  - src/query/create_tables.sql
    - File with DDL used to create the tables in PostgreSQL.
  - src/test
    -  Folder for unit testing code
  - src/metadata.py
    - Python Application to get the data from the Api transform and put it into the  PostgreSQL tables.

-DockerFile
-docker-compose.yml
-requirements.txt
  

### Source
- https://openweathermap.org/
  
### Tecnologies: 
- ➡️ 🐍 [Python](#-python)
- ➡️[Docker](#-docker)
- ➡️ [PostgreSQL](#-postgres)





