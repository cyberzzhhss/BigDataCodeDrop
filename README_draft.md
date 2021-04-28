# Source for Datasets:

1. The Food Violation record dataset (**rename** the file as **"boston_raw.csv"** before uploading to hdfs):
   
   The file updated on a daily basis. Our file version is on April 21.
   Your attempt to replicate the result might be different if you choose a dataset from a different date.
   The raw file from April 21 is stored as boston_raw.csv inside the peel server.

   https://data.boston.gov/dataset/food-establishment-inspections/resource/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c

2. The yelp dataset (rename "yelp_business.json"):
   https://www.yelp.com/dataset/download

   * Download from the JSON section
   * After downloading, unzip **yelp_dataset.tar**
   * Go inside yelp_dataset folder
   * yelp_academic_dataset_business.json should be modified at this date: January 28, 2021 at 2:06 PM
   * yelp_academic_dataset_business.json should be 124.4 MB large
   * **rename** **"yelp_academic_dataset_business.json"** as **"yelp_business.json"**
   

# Dataset 1

## Preparation 

commands
```

ls
hdfs dfs -rm -r -f hiveInput 
hdfs dfs -ls 
hdfs dfs -mkdir hiveInput
hdfs dfs -put boston_raw.csv hiveInput
hdfs dfs -ls hiveInput
```

response
```
[zs1113@hlog-1 ~]$ hdfs dfs -rm -r -f hiveInput 
21/04/21 19:21:27 INFO fs.TrashPolicyDefault: Moved: 'hdfs://horton.hpc.nyu.edu:8020/user/zs1113/hiveInput' to trash at: hdfs://horton.hpc.nyu.edu:8020/user/zs1113/.Trash/Current/user/zs1113/hiveInput
[zs1113@hlog-1 ~]$ hdfs dfs -ls 
Found 1 items
drwx------+  - zs1113 zs1113          0 2021-04-21 19:21 .Trash
[zs1113@hlog-1 ~]$ hdfs dfs -mkdir hiveInput
[zs1113@hlog-1 ~]$ hdfs dfs -put boston_raw.csv hiveInput
[zs1113@hlog-1 ~]$ hdfs dfs -ls hiveInput
Found 1 items
-rw-rw----+  3 zs1113 zs1113  249865148 2021-04-21 19:21 hiveInput/boston_raw.csv
[zs1113@hlog-1 ~]$ 
```

log into hive
```
beeline --silent
!connect jdbc:hive2://hm-1.hpc.nyu.edu:10000/
[NetID]
[PassCode]
use [NetID];
```

Table 1 boston_raw
```
DROP TABLE IF EXISTS boston_raw;

CREATE EXTERNAL TABLE boston_raw (businessname STRING,dbaname STRING,legalowner STRING,namelast STRING,namefirst STRING,licenseno INT,issdttm STRING,expdttm STRING,licstatus STRING,licensecat STRING,descript STRING,result STRING,resultdttm STRING,violation STRING,viollevel STRING,violdesc STRING,violdttm STRING,violstatus STRING,statusdate STRING,comments STRING,address STRING,city STRING,state STRING,zip STRING,property_id INT, latitude STRING, longitude STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/zs1113/boston_raw'  tblproperties("skip.header.line.count"="1"); 

LOAD DATA INPATH '/user/zs1113/hiveInput/boston_raw.csv' INTO TABLE boston_raw;
```

```
SELECT COUNT(latitude) AS count, LENGTH(latitude) AS str_length FROM boston_raw GROUP BY LENGTH(latitude);

+---------+-------------+
|  count  | str_length  |
+---------+-------------+
| 363990  | 0           |
| 925746  | 14          |
+---------+-------------+
```

```
SELECT COUNT(longitude) AS count, LENGTH(longitude) AS str_length FROM boston_raw GROUP BY LENGTH(longitude);

+---------+-------------+
|  count  | str_length  |
+---------+-------------+
| 0       | NULL        |
| 925746  | 16          |
+---------+-------------+
```

```
SELECT substring(latitude,3,14), latitude FROM boston_raw limit 3;

+---------------+-----------------+
|      _c0      |    latitude     |
+---------------+-----------------+
| 42.278590000  | "(42.278590000  |
| 42.278590000  | "(42.278590000  |
| 42.278590000  | "(42.278590000  |
+---------------+-----------------+
```

```
SELECT substring(longitude,1,14), longitude FROM boston_raw limit 3;

+-----------------+-------------------+
|       _c0       |     longitude     |
+-----------------+-------------------+
|  -71.119440000  |  -71.119440000)"  |
|  -71.119440000  |  -71.119440000)"  |
|  -71.119440000  |  -71.119440000)"  |
+-----------------+-------------------+
```

Table 2 boston_clean created from table 1 boston_raw
```
DROP TABLE IF EXISTS boston_clean;

CREATE TABLE boston_clean AS SELECT businessname AS name, address, city, result, SUBSTRING(latitude,3,14) AS latitude, SUBSTRING(longitude,2,13) AS longitude, property_id FROM boston_raw WHERE (LENGTH(latitude) > 9 AND LENGTH(longitude) > 9);
```

```
DESCRIBE boston_clean;

+--------------+------------+----------+
|   col_name   | data_type  | comment  |
+--------------+------------+----------+
| name         | string     |          |
| address      | string     |          |
| city         | string     |          |
| result       | string     |          |
| latitude     | string     |          |
| longitude    | string     |          |
| property_id  | int        |          |
+--------------+------------+----------+
```


```
SELECT COUNT(*) AS count FROM boston_clean;

+---------+
|  count  |
+---------+
| 925746  |
+---------+
```

# Dataset 2

## Preparation 

commands
```
ls
hdfs dfs -rm -r -f hiveInput 
hdfs dfs -ls 
hdfs dfs -mkdir hiveInput
hdfs dfs -put yelp_business.json hiveInput
hdfs dfs -ls hiveInput
```

response
```
[zs1113@hlog-1 ~]$ hdfs dfs -rm -r -f hiveInput 
21/04/21 19:23:58 INFO fs.TrashPolicyDefault: Moved: 'hdfs://horton.hpc.nyu.edu:8020/user/zs1113/hiveInput' to trash at: hdfs://horton.hpc.nyu.edu:8020/user/zs1113/.Trash/Current/user/zs1113/hiveInput1619047438246
[zs1113@hlog-1 ~]$ hdfs dfs -ls 
Found 2 items
drwx------+  - zs1113 zs1113          0 2021-04-21 19:21 .Trash
drwxrwx---+  - hive   zs1113          0 2021-04-27 19:30 boston_raw
[zs1113@hlog-1 ~]$ hdfs dfs -mkdir hiveInput
[zs1113@hlog-1 ~]$ hdfs dfs -put yelp_business.json hiveInput
[zs1113@hlog-1 ~]$ hdfs dfs -ls hiveInput
Found 1 items
-rw-rw----+  3 zs1113 zs1113  124380583 2021-04-21 19:24 hiveInput/yelp_business.json
[zs1113@hlog-1 ~]$ hdfs dfs -ls
Found 3 items
drwx------+  - zs1113 zs1113          0 2021-04-21 19:21 .Trash
drwxrwx---+  - hive   zs1113          0 2021-04-21 19:30 boston_raw
drwxrwx---+  - zs1113 zs1113          0 2021-04-21 19:45 hiveInput
```

log into hive
```
beeline --silent
!connect jdbc:hive2://hm-1.hpc.nyu.edu:10000/
[NetID]
[PassCode]
use [NetID];
```

table 3 json_tab
```
DROP TABLE IF EXISTS json_tab;
CREATE TABLE json_tab(col1 string);
LOAD DATA INPATH '/user/zs1113/hiveInput/yelp_business.json' INTO TABLE json_tab;
```

```
DESCRIBE json_tab;

+-----------+------------+----------+
| col_name  | data_type  | comment  |
+-----------+------------+----------+
| col1      | string     |          |
+-----------+------------+----------+
```

```
SELECT * FROM json_tab LIMIT 1;

+----------------------------------------------------+
|                   json_tab.col1                    |
+----------------------------------------------------+
| {"business_id":"6iYb2HFDywm3zjuRg0shjw","name":"Oskar Blues Taproom","address":"921 Pearl St","city":"Boulder","state":"CO","postal_code":"80302","latitude":40.0175444,"longitude":-105.2833481,"stars":4.0,"review_count":86,"is_open":1,"attributes":{"RestaurantsTableService":"True","WiFi":"u'free'","BikeParking":"True","BusinessParking":"{'garage': False, 'street': True, 'validated': False, 'lot': False, 'valet': False}","BusinessAcceptsCreditCards":"True","RestaurantsReservations":"False","WheelchairAccessible":"True","Caters":"True","OutdoorSeating":"True","RestaurantsGoodForGroups":"True","HappyHour":"True","BusinessAcceptsBitcoin":"False","RestaurantsPriceRange2":"2","Ambience":"{'touristy': False, 'hipster': False, 'romantic': False, 'divey': False, 'intimate': False, 'trendy': False, 'upscale': False, 'classy': False, 'casual': True}","HasTV":"True","Alcohol":"'beer_and_wine'","GoodForMeal":"{'dessert': False, 'latenight': False, 'lunch': False, 'dinner': False, 'brunch': False, 'breakfast': False}","DogsAllowed":"False","RestaurantsTakeOut":"True","NoiseLevel":"u'average'","RestaurantsAttire":"'casual'","RestaurantsDelivery":"None"},"categories":"Gastropubs, Food, Beer Gardens, Restaurants, Bars, American (Traditional), Beer Bar, Nightlife, Breweries","hours":{"Monday":"11:0-23:0","Tuesday":"11:0-23:0","Wednesday":"11:0-23:0","Thursday":"11:0-23:0","Friday":"11:0-23:0","Saturday":"11:0-23:0","Sunday":"11:0-23:0"}} |
+----------------------------------------------------+
```

table 4 yelp_business created from table 3 json_tab
```
DROP TABLE IF EXISTS yelp_business;
CREATE TABLE yelp_business(business_id STRING, name STRING, address STRING, city STRING, stars DECIMAL(2,1), review_count INT, is_open INT, latitude STRING, longitude STRING);
```

```
INSERT OVERWRITE TABLE yelp_business SELECT GET_JSON_OBJECT(col1, '$.business_id'), GET_JSON_OBJECT(col1, '$.name'), GET_JSON_OBJECT(col1, '$.address'), GET_JSON_OBJECT(col1, '$.city'), GET_JSON_OBJECT(col1, '$.stars'), GET_JSON_OBJECT(col1, '$.review_count'), GET_JSON_OBJECT(col1, '$.is_open'), GET_JSON_OBJECT(col1, '$.latitude'), GET_JSON_OBJECT(col1, '$.longitude') FROM json_tab;
```

```
SELECT COUNT(*) AS count from yelp_business;

+---------+
|  count  |
+---------+
| 160585  |
+---------+
```

```
DESCRIBE yelp_business;

+---------------+---------------+----------+
|   col_name    |   data_type   | comment  |
+---------------+---------------+----------+
| business_id   | string        |          |
| name          | string        |          |
| address       | string        |          |
| city          | string        |          |
| stars         | decimal(2,1)  |          |
| review_count  | int           |          |
| is_open       | int           |          |
| latitude      | string        |          |
| longitude     | string        |          |
+---------------+---------------+----------+
```

```
SELECT COUNT(*) AS count, city FROM yelp_business GROUP BY city ORDER BY count DESC LIMIT 10;

+--------+------------+
| count  |    city    |
+--------+------------+
| 22416  | Austin     |
| 18203  | Portland   |
| 13330  | Vancouver  |
| 12612  | Atlanta    |
| 10637  | Orlando    |
| 8263   | Boston     |
| 6634   | Columbus   |
| 2542   | Boulder    |
| 2433   | Cambridge  |
| 2252   | Beaverton  |
+--------+------------+
```


```
SELECT COUNT(*) as count, result FROM boston_clean GROUP BY result ORDER BY count DESC;

+---------+-------------+
|  count  |   result    |
+---------+-------------+
| 395376  | HE_Fail     |
| 287364  | HE_Pass     |
| 111846  | HE_Filed    |
| 68712   | HE_FailExt  |
| 32388   | HE_Hearing  |
| 13532   | HE_NotReq   |
| 10222   | HE_TSOP     |
| 3020    | HE_OutBus   |
| 1032    | HE_Closure  |
| 1006    | Pass        |
| 514     | HE_VolClos  |
| 326     | Fail        |
| 180     | HE_FAILNOR  |
| 148     | HE_Misc     |
| 44      | DATAERR     |
| 30      | HE_Hold     |
| 4       | PassViol    |
| 2       | Closed      |
+---------+-------------+
```

```
SELECT name, address, city, latitude, longitude, SUM(CASE result WHEN 'HE_Pass' THEN 1 WHEN 'Pass' THEN 1 ELSE 0 END) AS n_pass, SUM(CASE result WHEN 'HE_Pass' THEN 0 ELSE 1 END) AS n_fail, ROUND(SUM(CASE result WHEN 'HE_Pass' THEN 1 ELSE 0 END)/COUNT(result), 5) AS pass_rate FROM boston_clean GROUP BY name, address, city, latitude, longitude ORDER BY n_pass DESC LIMIT 5;

+------------------+-------------------------+--------------+---------------+-----------------+---------+---------+------------+
|       name       |         address         |     city     |   latitude    |    longitude    | n_pass  | n_fail  | pass_rate  |
+------------------+-------------------------+--------------+---------------+-----------------+---------+---------+------------+
| BALE RESTAURANT  | 1052   Dorchester AVE   | Dorchester   | 42.314791000  |  -71.056611000  | 568     | 902     | 0.38639    |
| EL CHALAN        | 405   Chelsea ST        | East Boston  | 42.379767000  |  -71.027000000  | 514     | 896     | 0.36454    |
| India Quality    | 484   Commonwealth AVE  | Boston       | 42.348540000  |  -71.094220000  | 514     | 984     | 0.34312    |
| Great Chef       | 390   Chelsea ST        | East Boston  | 42.379493000  |  -71.027910000  | 504     | 628     | 0.44523    |
| ORIENTAL HOUSE   | 560   Washington ST     | Dorchester   | 42.292072000  |  -71.071521000  | 504     | 1008    | 0.33333    |
+------------------+-------------------------+--------------+---------------+-----------------+---------+---------+------------+
```

Table 5 boston_health created from table 2 boston_clean

```
DROP TABLE IF EXISTS boston_health;

CREATE TABLE boston_health AS SELECT name, address, city, latitude, longitude, SUM(CASE result WHEN 'HE_Pass' THEN 1 ELSE 0 END) AS n_pass, SUM(CASE result WHEN 'HE_Pass' THEN 0 ELSE 1 END) AS n_fail, ROUND(SUM(CASE result WHEN 'HE_Pass' THEN 1 ELSE 0 END)/COUNT(result), 5) AS pass_rate FROM boston_clean GROUP BY name, address, city, latitude, longitude;
```

```
DESCRIBE boston_health;

+------------+------------+----------+
|  col_name  | data_type  | comment  |
+------------+------------+----------+
| name       | string     |          |
| address    | string     |          |
| city       | string     |          |
| latitude   | string     |          |
| longitude  | string     |          |
| n_pass     | bigint     |          |
| n_fail     | bigint     |          |
| pass_rate  | double     |          |
+------------+------------+----------+
```

boston_health

```
SELECT COUNT(*) AS COUNT FROM boston_health;
+--------+
| count  |
+--------+
| 5518   |
+--------+
```


Table 6 boston_stats (stats combined yelp_busines and boston_health )

Table 6 boston_stats created from joining table 5 boston_health and table 4 yelp_business

```
SELECT SUBSTRING(longitude, 1, 7) AS longitude, SUBSTRING(latitude, 1, 6) AS latitude FROM boston_health LIMIT 5;
+------------+-----------+
| longitude  | latitude  |
+------------+-----------+
|  -71.055   | 42.352    |
|  -71.068   | 42.362    |
|  -71.113   | 42.312    |
|  -71.058   | 42.359    |
|  -71.066   | 42.352    |
+------------+-----------+

```

name 3
address 3
lat 5
long 5 (including minus sign, 6)

```
DROP TABLE IF EXISTS temp;

CREATE TABLE temp AS SELECT yelp_business.business_id, yelp_business.name, yelp_business.address, yelp_business.city, yelp_business.stars, yelp_business.review_count, yelp_business.is_open, boston_health.n_pass, boston_health.n_fail, boston_health.pass_rate, yelp_business.latitude, yelp_business.longitude FROM yelp_business INNER JOIN boston_health ON UPPER(SUBSTRING(yelp_business.name, 1, 2))=UPPER(SUBSTRING(boston_health.name, 1, 2)) AND UPPER(SUBSTRING(yelp_business.address, 1, 2))=UPPER(SUBSTRING(boston_health.address, 1, 2)) AND SUBSTRING(yelp_business.latitude, 1, 5)=SUBSTRING(boston_health.latitude, 1, 5) AND SUBSTRING(yelp_business.longitude, 1, 6)=SUBSTRING(boston_health.longitude, 1, 6);

SELECT COUNT(DISTINCT business_id) AS n, COUNT(business_id) AS total,  COUNT(DISTINCT business_id)/COUNT(business_id) distinct_rate FROM temp;

+-------+--------+------------------+
|   n   | total  | uniqueness_rate  |
+-------+--------+------------------+
| 2761  | 3520   | 0.784375         | 
+-------+--------+------------------+
```

select business_id that only appears once.
```
DROP TABLE IF EXISTS unique_temp;

CREATE TABLE unique_temp AS SELECT business_id  FROM temp GROUP BY business_id HAVING (COUNT(business_id) < 2);

SELECT COUNT(DISTINCT business_id) AS n, COUNT(business_id) AS total,  COUNT(DISTINCT business_id)/COUNT(business_id) uniqueness_rate FROM unique_temp;

+-------+--------+------------------+
|   n   | total  | uniqueness_rate  |
+-------+--------+------------------+
| 2260  | 2260   | 1.0              |
+-------+--------+------------------+
```

selecting business with addresss excluding comma sign for later analysis

```
DROP TABLE IF EXISTS boston_stats;

CREATE TABLE boston_stats AS SELECT unique_temp.business_id, temp.name, temp.address, temp.city, temp.stars, temp.review_count, temp.is_open, temp.n_pass, temp.n_fail, temp.pass_rate, temp.latitude, temp.longitude FROM unique_temp LEFT JOIN temp ON unique_temp.business_id=temp.business_id WHERE temp.address NOT LIKE '%,%' AND temp.name NOT LIKE '%,%';

SELECT COUNT(DISTINCT business_id) AS n, COUNT(business_id) AS total,  COUNT(DISTINCT business_id)/COUNT(business_id) unique_rate FROM boston_stats;
+-------+--------+--------------+
|   n   | total  | unique_rate  |
+-------+--------+--------------+
| 2157  | 2157   | 1.0          |
+-------+--------+--------------+
```
```
DESCRIBE boston_stats;
+---------------+---------------+----------+
|   col_name    |   data_type   | comment  |
+---------------+---------------+----------+
| business_id   | string        |          |
| name          | string        |          |
| address       | string        |          |
| city          | string        |          |
| stars         | decimal(2,1)  |          |
| review_count  | int           |          |
| is_open       | int           |          |
| n_pass        | bigint        |          |
| n_fail        | bigint        |          |
| pass_rate     | double        |          |
| latitude      | string        |          |
| longitude     | string        |          |
+---------------+---------------+----------+
```

## Export Data

```
INSERT OVERWRITE DIRECTORY '/user/zs1113/hiveOutput' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * FROM boston_stats;
```
See at peel server

### commands
```
hdfs dfs -ls
hdfs dfs -ls hiveOutput
hdfs dfs -get hiveOutput/000000_0
```
### response

```
[zs1113@hlog-2 ~]$ hdfs dfs -ls
Found 4 items
drwx------+  - zs1113 zs1113          0 2021-04-22 00:50 .Trash
drwxrwx---+  - hive   zs1113          0 2021-04-21 16:28 boston_raw
drwxrwx---+  - zs1113 zs1113          0 2021-04-21 19:27 hiveInput
drwxrwx---+  - hive   zs1113          0 2021-04-22 00:50 hiveOutput
[zs1113@hlog-2 ~]$ hdfs dfs -ls hiveOutput
Found 1 items
-rwxrwx---+  3 hive zs1113     201697 2021-04-22 00:50 hiveOutput/000000_0
[zs1113@hlog-2 ~]$ hdfs dfs -get hiveOutput/000000_0
[zs1113@hlog-2 ~]$ 
```

See at peel server
### commands
```
ls
```
### response
```
000000_0  boston_health.csv  yelp_business.json 
```
## Obstacles

1. Spliting cooridnates for boston_clean

2. JSON conversion for yelp_business

3. Joining table, two source tables are not formatted in the same way

4. Joining table, adjusting the input to make sure the tables are merged properly.

BEFORE

```
select substring(longitude,1,2), substring(latitude,1,2) from yelp_business limit 5;

+------+------+
| _c0  | _c1  |
+------+------+
| -1   | 40   |
| -1   | 45   |
| -1   | 45   |
| -8   | 28   |
| -8   | 33   |
+------+------+

select substring(longitude,1,2), substring(latitude,1,2) from boston_health limit 5;

+------+------+
| _c0  | _c1  |
+------+------+
|  -   | 42   |
|  -   | 42   |
|  -   | 42   |
|  -   | 42   |
|  -   | 42   |
+------+------+

```
PROBLEM

BEFORE
```
CREATE TABLE boston_clean AS SELECT businessname AS name, address, city, result, SUBSTRING(latitude,3,14) AS latitude, SUBSTRING(longitude,1,14) AS longitude, property_id FROM boston_raw WHERE (LENGTH(latitude) > 9 AND LENGTH(longitude) > 9);
```
AFTER
```
CREATE TABLE boston_clean AS SELECT businessname AS name, address, city, result, SUBSTRING(latitude,3,14) AS latitude, SUBSTRING(longitude,2,14) AS longitude, property_id FROM boston_raw WHERE (LENGTH(latitude) > 9 AND LENGTH(longitude) > 9);
```
COMMENT 
```
SUBSTRING(longitude,1,14) ===> SUBSTRING(longitude,2,14)
```

```
select substring(longitude,1,2), substring(latitude,1,2) from yelp_business limit 5;

+------+------+
| _c0  | _c1  |
+------+------+
| -1   | 40   |
| -1   | 45   |
| -1   | 45   |
| -8   | 28   |
| -8   | 33   |
+------+------+
select substring(longitude,1,2), substring(latitude,1,2) from boston_health limit 5;

+------+------+
| _c0  | _c1  |
+------+------+
| -7   | 42   |
| -7   | 42   |
| -7   | 42   |
| -7   | 42   |
| -7   | 42   |
+------+------+
```



4. Joining table, adjusting the input to make sure the tables are merged properly.

name 3
address 3
lat 5
long 5 (including minus sign, 6)

```
DROP TABLE test_table;

CREATE TABLE test_table AS SELECT yelp_business.business_id, yelp_business.name, yelp_business.address, yelp_business.city, yelp_business.stars, yelp_business.review_count, yelp_business.is_open, boston_health.n_pass, boston_health.n_fail, boston_health.pass_rate, yelp_business.latitude, yelp_business.longitude FROM yelp_business INNER JOIN boston_health ON UPPER(SUBSTRING(yelp_business.name, 1, 3))=UPPER(SUBSTRING(boston_health.name, 1, 3)) AND UPPER(SUBSTRING(yelp_business.address, 1, 3))=UPPER(SUBSTRING(boston_health.address, 1, 3)) AND SUBSTRING(yelp_business.latitude, 1, 5)=SUBSTRING(boston_health.latitude, 1, 5) AND SUBSTRING(yelp_business.longitude, 1, 6)=SUBSTRING(boston_health.longitude, 1, 6);

SELECT COUNT(DISTINCT business_id) AS n, COUNT(name) AS total,  COUNT(DISTINCT business_id)/COUNT(name) uniqueness_rate FROM test_table;

+-------+--------+---------------------+
|   n   | total  |   uniqueness_rate   |
+-------+--------+---------------------+
| 2031  | 2303   | 0.8818931828050369  | 2031 * 0.88 = 1787
+-------+--------+---------------------+

```

name 5
address 4
lat 5
long 5 (including minus sign, 6)

```
+-------+--------+---------------------+
|   n   | total  |   uniqueness_rate   |
+-------+--------+---------------------+
| 1423  | 1590   | 0.8949685534591195  | 1423 * 0.89 = 1266
+-------+--------+---------------------+
```


name 4
address 4
lat 4
long 3 (including minus sign, 3)
```
+-------+--------+---------------------+
|   n   | total  |   uniqueness_rate   |
+-------+--------+---------------------+
| 1644  | 1948   | 0.8439425051334702  | 1644 * 0.84 = 1380
+-------+--------+---------------------+
```
name 4
address 4
lat 5
long 5 (including minus sign, 6)

```
+-------+--------+---------------------+
|   n   | total  |   uniqueness_rate   |
+-------+--------+---------------------+
| 1492  | 1678   | 0.8891537544696066  | 1492 * 0.89 = 1327
+-------+--------+---------------------+
```
name 4
address 4
lat 6
long 6 (including minus sign, 7)

```
+-------+--------+---------------------+
|   n   | total  |   uniqueness_rate   |
+-------+--------+---------------------+
| 1226  | 1370   | 0.8948905109489051  | 1226 * 0.89 = 1091
+-------+--------+---------------------+
```

name 3
address 3
lat 6
long 6 (including minus sign, 7)
```
+-------+--------+---------------------+
|   n   | total  |   uniqueness_rate   |
+-------+--------+---------------------+
| 1666  | 1864   | 0.8937768240343348  | 1666 * 0.89 = 1482
+-------+--------+---------------------+
```

name 2
address 2
lat 6
long 6 (including minus sign, 7)

```
+-------+--------+---------------------+
|   n   | total  |   uniqueness_rate   |
+-------+--------+---------------------+
| 2015  | 2328   | 0.8655498281786942  | 2015 * 0.86 = 1732
+-------+--------+---------------------+
```

```

#REGRESSION IN SPARK

#Rename the exported data and upload to hdfs
ls
mv 000000_0 FinalData.csv
hdfs dfs -rm -r -f sparkInput 
hdfs dfs -ls 
hdfs dfs -mkdir sparkInput
hdfs dfs -put FinalData.csv sparkInput
hdfs dfs -ls sparkInput
```
```
#import data table onto spark as dataframe
[sx663@hlog-1 ~]$ spark-shell --deploy-mode client
scala> var df = spark.sql("SELECT * FROM csv.`/user/sx663/sparkInput/FinalData.csv`")
df: org.apache.spark.sql.DataFrame = [_c0: string, _c1: string ... 10 more fields]

##note:
_c0 is business_id  
_c1 is name
_c2 is address
_c3 is city
_c4 is stars
_c5 is review_count
_c6 is is_open
_c7 is n_pass
_c8 is n_fail
_c9 is pass_rate 
_c10 is latitude 
_c11 is longitude


#Extract specific columns for regression
##first regression: dependent variable: cleanliness(_c9 pass_rate); independent variable: ratings(_c4 stars)

scala> var partone = df.selectExpr("cast(_c9 as double) _c9","cast(_c4 as double) _c4")
partone: org.apache.spark.sql.DataFrame = [_c9: double, _c4: double]

scala> partone.printSchema()
root
 |-- _c9: double (nullable = true)
 |-- _c4: double (nullable = true)

#Set up proper format for Spark syntax
scala> import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorAssembler

##this is the independent variable "features"
scala> var assemblerone= new VectorAssembler().setInputCols(Array("_c4")).setOutputCol("features")
assemblerone: org.apache.spark.ml.feature.VectorAssembler = vecAssembler_c71fc44e20df

scala> partone =assemblerone.setHandleInvalid("skip").transform(partone)
partone: org.apache.spark.sql.DataFrame = [_c9: double, _c4: double ... 1 more field]

##this is the dependent variable "label"
scala> partone=partone.withColumnRenamed("_c9","label")
partone: org.apache.spark.sql.DataFrame = [label: double, _c4: double ... 1 more field]

#Perform Linear Regression()
##import, declare regression
scala> import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegression

scala> var lrone = new LinearRegression()
lrone: org.apache.spark.ml.regression.LinearRegression = linReg_0a63623fd5c0
##fit data
scala> var lrModelone = lrone.fit(partone)
21/04/23 14:50:52 WARN util.Instrumentation: [95bcd1fa] regParam is zero, which might cause numerical instability and overfitting.
[Stage 2:>                                                          (0 + 1) / 1]21/04/23 14:50:53 WARN netlib.BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS
21/04/23 14:50:53 WARN netlib.BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeRefBLAS
21/04/23 14:50:53 WARN netlib.LAPACK: Failed to load implementation from: com.github.fommil.netlib.NativeSystemLAPACK
21/04/23 14:50:53 WARN netlib.LAPACK: Failed to load implementation from: com.github.fommil.netlib.NativeRefLAPACK
lrModelone: org.apache.spark.ml.regression.LinearRegressionModel = linReg_0a63623fd5c0

#Get Results/assess goodness of fit
##Print out coefficients and intercepts
scala> println(s"Coefficients: ${lrModelone.coefficients} Intercept: ${lrModelone.intercept}")
Coefficients: [0.02988437905866312] Intercept: 0.2543787098282755

##evaluate regression
scala> var summaryone = lrModelone.summary
summaryone: org.apache.spark.ml.regression.LinearRegressionTrainingSummary = org.apache.spark.ml.regression.LinearRegressionTrainingSummary@7cdbc04

scala> println(s"r2: ${summaryone.r2}")
r2: 0.0156945754049429

##second regression: dependent variable: cleanliness(_c9 pass_rate); independent variable: online popularity(_c5 review counts)
scala> var parttwo = df.selectExpr("cast(_c9 as double) _c9","cast(_c5 as integer) _c5")
parttwo: org.apache.spark.sql.DataFrame = [_c9: double, _c5: int]

##same procedure in setting up regression
scala> var assemblertwo= new VectorAssembler().setInputCols(Array("_c5")).setOutputCol("features")
assemblertwo: org.apache.spark.ml.feature.VectorAssembler = vecAssembler_ccd76671ec27
##Print out coefficients and intercepts
scala> parttwo =assemblertwo.setHandleInvalid("skip").transform(parttwo)
parttwo: org.apache.spark.sql.DataFrame = [_c9: double, _c5: int ... 1 more field]

scala> parttwo=parttwo.withColumnRenamed("_c9","label")
parttwo: org.apache.spark.sql.DataFrame = [label: double, _c5: int ... 1 more field]

scala> var lrtwo = new LinearRegression()
lrtwo: org.apache.spark.ml.regression.LinearRegression = linReg_c68a99948646

scala> var lrModeltwo = lrtwo.fit(parttwo)
21/04/23 15:09:12 WARN util.Instrumentation: [51c53dae] regParam is zero, which might cause numerical instability and overfitting.
lrModeltwo: org.apache.spark.ml.regression.LinearRegressionModel = linReg_c68a99948646

##evaluate regression
println(s"Coefficients: ${lrModeltwo.coefficients} Intercept: ${lrModeltwo.intercept}")
Coefficients: [-0.007075239993046872] Intercept: 5.386810461496709

scala> var summarytwo = lrModeltwo.summary
summarytwo: org.apache.spark.ml.regression.LinearRegressionTrainingSummary = org.apache.spark.ml.regression.LinearRegressionTrainingSummary@1f2f7838

scala> println(s"r2: ${summarytwo.r2}")
r2: 0.00346864406594638

##third regression: dependent variable: ratings(_c4 stars); independent variable: online popularity(_c5 review counts)
scala> var partthree = df.selectExpr("cast(_c4 as double) _c4","cast(_c5 as integer) _c5")
partthree: org.apache.spark.sql.DataFrame = [_c4: double, _c5: int]

##same procedure in setting up regression
scala> var assemblerthree= new VectorAssembler().setInputCols(Array("_c5")).setOutputCol("features")
assemblerthree: org.apache.spark.ml.feature.VectorAssembler = vecAssembler_5ea6ca053585
scala> partthree =assemblerthree.setHandleInvalid("skip").transform(partthree)
partthree: org.apache.spark.sql.DataFrame = [_c4: double, _c5: int ... 1 more field]

scala> partthree=partthree.withColumnRenamed("_c4","label")
partthree: org.apache.spark.sql.DataFrame = [label: double, _c5: int ... 1 more field]

scala> var lrthree = new LinearRegression()
lrthree: org.apache.spark.ml.regression.LinearRegression = linReg_31c59b38f0cf

##evaluate regression

###somehow there's an error
var lrModelthree = lrthree.fit(partthree)

println(s"Coefficients: ${lrModelthree.coefficients} Intercept: ${lrModelthree.intercept}")


var summarythree = lrModelthree.summary


println(s"r2: ${summarythree.r2}")
```