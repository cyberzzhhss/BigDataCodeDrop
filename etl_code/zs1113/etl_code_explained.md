
<!--SQL to make the code have different colors in markdown editor, but the code is in fact in HiveSQL -->

# Recap on Data Ingest

From the previous data ingest, you should have 2 tables:
   * boston_raw
   * json_tab

After performing the following instructions, you should obtain 3 new tables:

* boston_clean
* yelp_business
* boston_stats


# Clean dataset 1 (boston dataset)

Table boston_clean created from table boston_raw, filter out null value and select targeted substring

```sql
DROP TABLE IF EXISTS boston_clean;

CREATE TABLE boston_clean AS SELECT businessname AS name, address, city, result, SUBSTRING(latitude,3,14) AS latitude, SUBSTRING(longitude,2,13) AS longitude, property_id FROM boston_raw WHERE (LENGTH(latitude) > 9 AND LENGTH(longitude) > 9);
```

## Explanation

some row does not have latitude value
```sql
SELECT COUNT(latitude) AS count, LENGTH(latitude) AS str_length FROM boston_raw GROUP BY LENGTH(latitude);

+---------+-------------+
|  count  | str_length  |
+---------+-------------+
| 363990  | 0           |
| 925746  | 14          |
+---------+-------------+
```
some row does not have longitude value
```sql
SELECT COUNT(longitude) AS count, LENGTH(longitude) AS str_length FROM boston_raw GROUP BY LENGTH(longitude);

+---------+-------------+
|  count  | str_length  |
+---------+-------------+
| 0       | NULL        |
| 925746  | 16          |
+---------+-------------+
```

Clean the latitude and longitude column by selecting the subtring

```sql
SELECT substring(latitude,3,14), latitude FROM boston_raw limit 3;

-- +---------------+-----------------+
-- |      _c0      |    latitude     |
-- +---------------+-----------------+
-- | 42.278590000  | "(42.278590000  |
-- | 42.278590000  | "(42.278590000  |
-- | 42.278590000  | "(42.278590000  |
-- +---------------+-----------------+
```

```sql
SELECT substring(longitude,2,13), longitude FROM boston_raw limit 3;

-- +-----------------+-------------------+
-- |       _c0       |     longitude     |
-- +-----------------+-------------------+
-- |  -71.119440000  |  -71.119440000)"  |
-- |  -71.119440000  |  -71.119440000)"  |
-- |  -71.119440000  |  -71.119440000)"  |
-- +-----------------+-------------------+
```

# Clean dataset 2 (yelp dataset)

View the table json_tab
```sql
SELECT * FROM json_tab LIMIT 1;

-- +----------------------------------------------------+
-- |                   json_tab.col1                    |
-- +----------------------------------------------------+
-- | {"business_id":"6iYb2HFDywm3zjuRg0shjw","name":"Oskar Blues Taproom","address":"921 Pearl St","city":"Boulder","state":"CO","postal_code":"80302","latitude":40.0175444,"longitude":-105.2833481,"stars":4.0,"review_count":86,"is_open":1,"attributes":{"RestaurantsTableService":"True","WiFi":"u'free'","BikeParking":"True","BusinessParking":"{'garage': False, 'street': True, 'validated': False, 'lot': False, 'valet': False}","BusinessAcceptsCreditCards":"True","RestaurantsReservations":"False","WheelchairAccessible":"True","Caters":"True","OutdoorSeating":"True","RestaurantsGoodForGroups":"True","HappyHour":"True","BusinessAcceptsBitcoin":"False","RestaurantsPriceRange2":"2","Ambience":"{'touristy': False, 'hipster': False, 'romantic': False, 'divey': False, 'intimate': False, 'trendy': False, 'upscale': False, 'classy': False, 'casual': True}","HasTV":"True","Alcohol":"'beer_and_wine'","GoodForMeal":"{'dessert': False, 'latenight': False, 'lunch': False, 'dinner': False, 'brunch': False, 'breakfast': False}","DogsAllowed":"False","RestaurantsTakeOut":"True","NoiseLevel":"u'average'","RestaurantsAttire":"'casual'","RestaurantsDelivery":"None"},"categories":"Gastropubs, Food, Beer Gardens, Restaurants, Bars, American (Traditional), Beer Bar, Nightlife, Breweries","hours":{"Monday":"11:0-23:0","Tuesday":"11:0-23:0","Wednesday":"11:0-23:0","Thursday":"11:0-23:0","Friday":"11:0-23:0","Saturday":"11:0-23:0","Sunday":"11:0-23:0"}} |
-- +----------------------------------------------------+

```

Create empty table yelp_business
```sql
DROP TABLE IF EXISTS yelp_business;

CREATE TABLE yelp_business(business_id STRING, name STRING, address STRING, city STRING, stars DECIMAL(2,1), review_count INT, is_open INT, latitude STRING, longitude STRING);
```

Load data into yelp_business by reading table json_tab, invoking GET_JSON_OBJECT function and selecting targeted column
```sql
INSERT OVERWRITE TABLE yelp_business SELECT GET_JSON_OBJECT(col1, '$.business_id'), GET_JSON_OBJECT(col1, '$.name'), GET_JSON_OBJECT(col1, '$.address'), GET_JSON_OBJECT(col1, '$.city'), GET_JSON_OBJECT(col1, '$.stars'), GET_JSON_OBJECT(col1, '$.review_count'), GET_JSON_OBJECT(col1, '$.is_open'), GET_JSON_OBJECT(col1, '$.latitude'), GET_JSON_OBJECT(col1, '$.longitude') FROM json_tab;
```

## Table Structure

```sql
DESCRIBE boston_clean;
-- +--------------+------------+----------+
-- |   col_name   | data_type  | comment  |
-- +--------------+------------+----------+
-- | name         | string     |          |
-- | address      | string     |          |
-- | city         | string     |          |
-- | result       | string     |          |
-- | latitude     | string     |          |
-- | longitude    | string     |          |
-- | property_id  | int        |          |
-- +--------------+------------+----------+
```

```sql
 DESCRIBE yelp_business;
-- +---------------+---------------+----------+
-- |   col_name    |   data_type   | comment  |
-- +---------------+---------------+----------+
-- | business_id   | string        |          |
-- | name          | string        |          |
-- | address       | string        |          |
-- | city          | string        |          |
-- | stars         | decimal(2,1)  |          |
-- | review_count  | int           |          |
-- | is_open       | int           |          |
-- | latitude      | string        |          |
-- | longitude     | string        |          |
-- +---------------+---------------+----------+
```


# Merge boston_clean and yelp_business
Because Hive does not support subquery, temporary tables are needed.

After trial and error, a **matching** combination of
   * 2 characters in the name, 
   * 2 characters in the address, 
   * 2 digits after the decimal point (5 characters in total) in latitude,
   * 2 digits after the decimal point (6 characters in total, including minus sign) in longitude

has the **best** performance.

Create first temporary table by matching relevant results

```sql
DROP TABLE IF EXISTS temp;

CREATE TABLE temp AS SELECT yelp_business.business_id, yelp_business.name, yelp_business.address, yelp_business.city, yelp_business.stars, yelp_business.review_count, yelp_business.is_open, boston_health.n_pass, boston_health.n_fail, boston_health.pass_rate, yelp_business.latitude, yelp_business.longitude FROM yelp_business INNER JOIN boston_health ON UPPER(SUBSTRING(yelp_business.name, 1, 2))=UPPER(SUBSTRING(boston_health.name, 1, 2)) AND UPPER(SUBSTRING(yelp_business.address, 1, 2))=UPPER(SUBSTRING(boston_health.address, 1, 2)) AND SUBSTRING(yelp_business.latitude, 1, 5)=SUBSTRING(boston_health.latitude, 1, 5) AND SUBSTRING(yelp_business.longitude, 1, 6)=SUBSTRING(boston_health.longitude, 1, 6);

```

Remove business_id that appears more than once 

```sql
DROP TABLE IF EXISTS unique_temp;

CREATE TABLE unique_temp AS SELECT business_id  FROM temp GROUP BY business_id HAVING (COUNT(business_id) < 2);

```

Create the desired table by joining the original yelp_business with unique_temp table
At the same time, removing the rows with comma sign in either name or address to avoid mistakes for later analytics.

```sql
DROP TABLE IF EXISTS boston_stats;

CREATE TABLE boston_stats AS SELECT unique_temp.business_id, temp.name, temp.address, temp.city, temp.stars, temp.review_count, temp.is_open, temp.n_pass, temp.n_fail, temp.pass_rate, temp.latitude, temp.longitude FROM unique_temp LEFT JOIN temp ON unique_temp.business_id=temp.business_id WHERE temp.address NOT LIKE '%,%' AND temp.name NOT LIKE '%,%';
```

The ultimate table boston_stats structure

```sql
DESCRIBE boston_stats;

-- +---------------+---------------+----------+
-- |   col_name    |   data_type   | comment  |
-- +---------------+---------------+----------+
-- | business_id   | string        |          |
-- | name          | string        |          |
-- | address       | string        |          |
-- | city          | string        |          |
-- | stars         | decimal(2,1)  |          |
-- | review_count  | int           |          |
-- | is_open       | int           |          |
-- | n_pass        | bigint        |          |
-- | n_fail        | bigint        |          |
-- | pass_rate     | double        |          |
-- | latitude      | string        |          |
-- | longitude     | string        |          |
-- +---------------+---------------+----------+
```

## Further explanation 

### Explanation of temp, unique_temp and boston_stats


According to yelp, business_id is **unique**; therefore, for boston_stats, the table is created by selecting for business_id that **appears only once**.

These results shows why the number of unique_match and the number distinct business are **different**.

This query finds out the number of distinct business_id and the total number of business_id.
```sql
SELECT COUNT(DISTINCT business_id) AS n, COUNT(business_id) AS total,  COUNT(DISTINCT business_id)/COUNT(business_id) distinct_rate FROM temp;
-- +-------+--------+----------------+
-- |   n   | total  | distinct_rate  |
-- +-------+--------+----------------+
-- | 2761  | 3520   | 0.784375       |
-- +-------+--------+----------------+
```

This query finds out the number of business_id that appears only once.
```sql
SELECT COUNT(unique_temp.business_id) AS unique, count(temp.business_id) AS total,  count(unique_temp.business_id)/count(temp.business_id) AS unique_match_rate FROM unique_temp FULL OUTER JOIN temp ON unique_temp.business_id = temp.business_id;

-- +---------+--------+---------------------+
-- | unique  | total  |  unique_match_rate  |
-- +---------+--------+---------------------+
-- | 2260    | 3520   | 0.6420454545454546  |
-- +---------+--------+---------------------+
```

Comment: 

2260 = unique match, the number of business_id that **appears only once**.

2761 = distinct business_id, the number of **different** business_id.
