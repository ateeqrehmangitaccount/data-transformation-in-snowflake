--select role to execute database level object
--select the default role SYSADMIN and use warehouse COMPUTE_WH on upper right side menu
USE ROLE SYSADMIN;

--select computing warehouse
USE WAREHOUSE COMPUTE_WH;

--create databas
CREATE DATABASE CITIBIKE;

--create table in citibike database

USE DATABASE CITIBIKE;


--create table table
--database table can be created by selecting database and schema from left side menu
--table can be created by using right side upper menu button by select appropriate option.

create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

select count(*) from trips;


--create external stage
--stage is bascially storage palce to store data file to loading an unloading data.
--create stage by select left side menu database schema and create stage
--create stage with some name (citibike_trips) any name can be used this name is used in snowflake       
--document example 
--create stage with the following link
--s3://snowflake-workshop-lab/citibike-trips-csv/


--query the datafiles from stage
list@citibike_trips;

--when query executed it will show all available datafiles in stage area

--create a file format to manage data according the data structure

create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format';

 --now test file format has been created

 show file formats in database citibike;

 --database and user related can be performed using snowsql
 --for exampel
  SHOW DATABASES -- will show all available databases

  -- load stage data into database for further data manuplation

copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;

  --it will take few seconds according the warehouse computation settings
  --when command executed successfully check the data has been loaded

 SELECT COUNT(*) FROM CITIBIKE.PUBLIC.TRIPS;

 --loading semi structured data

 --creating another database

 CREATE DATABASE WEATHER;

 --check weather database has been created
SHOW DATABASES;


--set database and schema
USE DATABASE WEATHER;

USE SCHEMA PUBLIC;

create table json_weather_data (v variant);

--create one external S3 stage to store datafile 
--select database WEATHER then select schema PUBLIC 
--from create option on upper right side select S3 stage

--nyc_weather --stage name
--s3://snowflake-workshop-lab/zero-weather-nyc

list@nyc_weather;

--create a file format for semi structured data as json

copy into json_weather_data
from @nyc_weather 
    file_format = (type = json strip_outer_array = true);


--check data has been loaded
select count(*) from json_weather_data;



--create a view to manage json data as structured data

create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from
    json_weather_data;


--we can describe the view with standard SQL command to view columns inside view
describe view json_weather_data_view;

--test some query that data has been loaded successfully.
select station_id,city_name from json_weather_data_view;



--now check the correlaton between weather and number of trips by joining two datasets in snowflake


select weather_conditions as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;

/* now it can be answered any business question from the two data sets by combining theme all
together
*/