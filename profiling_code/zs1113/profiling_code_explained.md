
<!--SQL to make the code have different colors in markdown editor, but the code is in fact in HiveSQL -->

# Goal

We will explore 3 tables:

* boston_clean
* yelp_business
* boston_stats

# Notice

In profiling_code.txt contains all the code; therefore, avoiding the need to copy the command line by line.

# Part 0

Log in to Hive

```
beeline --silent
!connect jdbc:hive2://hm-1.hpc.nyu.edu:10000/
[NetID]
[PassCode]
use [NetID];
```

# 3 STEPS IN TOTAL



# STEP 1




# Profiling boston_clean

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
Check total number of entry

```sql
SELECT COUNT(*) FROM boston_clean;
-- +----------+
-- |   _c0    |
-- +----------+
-- | 1851492  |
-- +----------+
```

Check boston and nearby city

```sql
SELECT COUNT(city) AS count, city FROM boston_clean GROUP BY city ORDER BY count DESC;
-- +---------+----------------+
-- |  count  |      city      |
-- +---------+----------------+
-- | 808204  | Boston         |
-- | 234700  | Dorchester     |
-- | 169384  | Roxbury        |
-- | 111716  | East Boston    |
-- | 103036  | Jamaica Plain  |
-- | 71116   | Allston        |
-- | 61456   | West Roxbury   |
-- | 58460   | Brighton       |
-- | 57368   | Roslindale     |
-- | 53432   | South Boston   |
-- | 33508   | Mattapan       |
-- | 32268   | Hyde Park      |
-- | 27408   | Mission Hill   |
-- | 27036   | Charlestown    |
-- | 1764    | Chestnut Hill  |
-- | 532     | South End      |
-- | 104     |                |
-- +---------+----------------+
```

Check the distribution of column result

```sql
SELECT COUNT(*) as count, result FROM boston_clean GROUP BY result ORDER BY count DESC;

-- +---------+-------------+
-- |  count  |   result    |
-- +---------+-------------+
-- | 395376  | HE_Fail     |
-- | 287364  | HE_Pass     |
-- | 111846  | HE_Filed    |
-- | 68712   | HE_FailExt  |
-- | 32388   | HE_Hearing  |
-- | 13532   | HE_NotReq   |
-- | 10222   | HE_TSOP     |
-- | 3020    | HE_OutBus   |
-- | 1032    | HE_Closure  |
-- | 1006    | Pass        |
-- | 514     | HE_VolClos  |
-- | 326     | Fail        |
-- | 180     | HE_FAILNOR  |
-- | 148     | HE_Misc     |
-- | 44      | DATAERR     |
-- | 30      | HE_Hold     |
-- | 4       | PassViol    |
-- | 2       | Closed      |
-- +---------+-------------+
```

Notice: [Only HE_Pass is health pass, other forms of pass is not considered a health pass]



# STEP 2





# Profiling yelp_business

```sql
SELECT COUNT(*) FROM yelp_business;
-- +---------+
-- |   _c0   |
-- +---------+
-- | 160585  |
-- +---------+

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

Find max, min, avg of stars for business

```sql
SELECT MAX(stars) AS max_star, MIN(stars) AS min_star, AVG(stars) AS avg_star FROM yelp_business;

-- +-----------+-----------+-----------+
-- | max_star  | min_star  | avg_star  |
-- +-----------+-----------+-----------+
-- | 5.0       | 1.0       | 3.65695   |
-- +-----------+-----------+-----------+
```

Find top 3 most occured stars for business, top 1 is the mode.

```sql
SELECT COUNT(stars) AS num, stars FROM yelp_business GROUP BY stars ORDER BY num DESC LIMIT 3;
-- +--------+--------+
-- |  num   | stars  |
-- +--------+--------+
-- | 34056  | 4.0    |
-- | 28835  | 3.5    |
-- | 28072  | 4.5    |
-- +--------+--------+

```

Find the number of open businesses and closed businesses

```sql
SELECT COUNT(is_open), is_open FROM yelp_business GROUP BY is_open;
-- +---------+----------+
-- |   _c0   | is_open  |
-- +---------+----------+
-- | 37337   | 0        |
-- | 123248  | 1        |
-- +---------+----------+
```

Find max, min, avg of review_count for business

```sql
SELECT MAX(review_count) AS max_review_count, MIN(review_count) AS min_review_count, AVG(review_count) AS avg_review_count FROM yelp_business;
-- +-------------------+-------------------+--------------------+
-- | max_review_count  | min_review_count  |  avg_review_count  |
-- +-------------------+-------------------+--------------------+
-- | 9185              | 5                 | 51.96454837002211  |
-- +-------------------+-------------------+--------------------+
```


Find top 3 most occured review_count for business, top 1 is the mode.

```sql
SELECT COUNT(review_count) AS num, review_count FROM yelp_business GROUP BY review_count ORDER BY num DESC LIMIT 3;

-- +--------+---------------+
-- |  num   | review_count  |
-- +--------+---------------+
-- | 13844  | 5             |
-- | 11224  | 6             |
-- | 9421   | 7             |
-- +--------+---------------+

```

Find the top 10 most occurred city in yelp_business

```sql
SELECT COUNT(city) AS COUNT, city FROM yelp_business GROUP BY city ORDER BY COUNT DESC LIMIT 10;

-- +--------+------------+
-- | count  |    city    |
-- +--------+------------+
-- | 22416  | Austin     |
-- | 18203  | Portland   |
-- | 13330  | Vancouver  |
-- | 12612  | Atlanta    |
-- | 10637  | Orlando    |
-- | 8263   | Boston     |
-- | 6634   | Columbus   |
-- | 2542   | Boulder    |
-- | 2433   | Cambridge  |
-- | 2252   | Beaverton  |
-- +--------+------------+
```
comment: only boston data has health information, the other cities do not have easily accessible health data. 





# STEP 3





# Profiling boston_stats


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

```sql
SELECT COUNT(*) FROM boston_stats;

-- +-------+
-- |  _c0  |
-- +-------+
-- | 2157  |
-- +-------+
```
Find max, min, avg of stars for business

```sql
SELECT MAX(stars) AS max_star, MIN(stars) AS min_star, AVG(stars) AS avg_star FROM boston_stats;

-- +-----------+-----------+-----------+
-- | max_star  | min_star  | avg_star  |
-- +-----------+-----------+-----------+
-- | 5.0       | 1.0       | 3.50765   |
-- +-----------+-----------+-----------+
```
Find top 3 most occured stars for business, top 1 is the mode.
```sql
SELECT COUNT(stars) AS num, stars FROM boston_stats GROUP BY stars ORDER BY num DESC LIMIT 3;

-- +------+--------+
-- | num  | stars  |
-- +------+--------+
-- | 588  | 4.0    |
-- | 559  | 3.5    |
-- | 372  | 3.0    |
-- +------+--------+
```

Find the number of open businesses and closed businesses

```sql
SELECT COUNT(is_open), is_open FROM boston_stats GROUP BY is_open;

```

Find top 3 most occured review_count for business, top 1 is the mode.
```sql
-- +-------+----------+
-- |  _c0  | is_open  |
-- +-------+----------+
-- | 781   | 0        |
-- | 1376  | 1        |
-- +-------+----------+
```

Find max, min, avg of review_count for business

```sql
SELECT MAX(review_count) AS max_review_count, MIN(review_count) AS min_review_count, AVG(review_count) AS avg_review_count FROM boston_stats;
-- +-------------------+-------------------+---------------------+
-- | max_review_count  | min_review_count  |  avg_review_count   |
-- +-------------------+-------------------+---------------------+
-- | 5115              | 5                 | 144.72508113120074  |
-- +-------------------+-------------------+---------------------+
```


Find top 3 most occured review_count for business, top 1 is the mode.

```sql
SELECT COUNT(review_count) AS num, review_count FROM boston_stats GROUP BY review_count ORDER BY num DESC LIMIT 3;
-- +------+---------------+
-- | num  | review_count  |
-- +------+---------------+
-- | 77   | 5             |
-- | 71   | 6             |
-- | 51   | 8             |
-- +------+---------------+

```

Find the top 10 most occurred city in boston_stats
```sql
SELECT COUNT(city) AS COUNT, city FROM boston_stats GROUP BY city ORDER BY COUNT DESC LIMIT 10;

-- +--------+----------------+
-- | count  |      city      |
-- +--------+----------------+
-- | 1538   | Boston         |
-- | 99     | Brighton       |
-- | 93     | Dorchester     |
-- | 88     | Allston        |
-- | 85     | Jamaica Plain  |
-- | 43     | Roslindale     |
-- | 40     | West Roxbury   |
-- | 33     | Roxbury        |
-- | 26     | East Boston    |
-- | 22     | Hyde Park      |
-- +--------+----------------+

```
comment: only boston data has health information, the other cities do not have easily accessible health data. 



# END (all steps are done)