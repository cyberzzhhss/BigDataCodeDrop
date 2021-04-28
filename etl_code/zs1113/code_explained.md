
<!--SQL to make the code have different colors in markdown editor, but the code is in fact in HiveSQL -->

# Recap on Data Ingest

From the previous data ingest, you should have 2 tables:
   * boston_raw
   * json_tab

After performing the following instructions, you should obtain 2 new tables:

* boston_clean
* yelp_business


# Clean dataset 1 (boston dataset)

Table boston_clean created from table boston_raw, filter out null value and select targeted substring

```sql
DROP TABLE IF EXISTS boston_clean;

CREATE TABLE boston_clean AS SELECT businessname AS name, address, city, result, SUBSTRING(latitude,3,14) AS latitude, SUBSTRING(longitude,2,14) AS longitude, property_id FROM boston_raw WHERE (LENGTH(latitude) > 9 AND LENGTH(longitude) > 9);
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
SELECT substring(longitude,1,14), longitude FROM boston_raw limit 3;

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