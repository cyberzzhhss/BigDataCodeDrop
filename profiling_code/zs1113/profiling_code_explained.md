
<!--SQL to make the code have different colors in markdown editor, but the code is in fact in HiveSQL -->

# Goal

We will explore **4** tables:

* boston_clean
* boston_health
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

# 4 STEPS IN TOTAL



# STEP 1




## Profiling boston_clean

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

Check fields
```sql
DESCRIBE boston_health;
-- +------------+------------+----------+
-- |  col_name  | data_type  | comment  |
-- +------------+------------+----------+
-- | name       | string     |          |
-- | address    | string     |          |
-- | city       | string     |          |
-- | latitude   | string     |          |
-- | longitude  | string     |          |
-- | n_pass     | bigint     |          |
-- | n_fail     | bigint     |          |
-- | pass_rate  | double     |          |
-- +------------+------------+----------+
```

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



## Profiling boston_health

```sql
DESCRIBE boston_health;
-- +------------+------------+----------+
-- |  col_name  | data_type  | comment  |
-- +------------+------------+----------+
-- | name       | string     |          |
-- | address    | string     |          |
-- | city       | string     |          |
-- | latitude   | string     |          |
-- | longitude  | string     |          |
-- | n_pass     | bigint     |          |
-- | n_fail     | bigint     |          |
-- | pass_rate  | double     |          |
-- +------------+------------+----------+
```


```sql
SELECT COUNT(*) FROM boston_health;
-- +-------+
-- |  _c0  |
-- +-------+
-- | 5518  |
-- +-------+

```

Find max, min, avg of n_pass
```sql
SELECT MAX(n_pass) AS max_n_pass, MIN(n_pass) AS min_n_pass, AVG(n_pass) AS avg_n_pass FROM boston_health;

-- +-------------+-------------+---------------------+
-- | max_n_pass  | min_n_pass  |     avg_n_pass      |
-- +-------------+-------------+---------------------+
-- | 2556        | 0           | 234.34903950706777  |
-- +-------------+-------------+---------------------+

```

Find the top 3 most occurred n_pass, top 1 occured is the mode
```sql
SELECT COUNT(n_pass) AS num, n_pass FROM boston_health GROUP BY n_pass ORDER BY num DESC LIMIT 3;

-- +------+---------+
-- | num  | n_pass  |
-- +------+---------+
-- | 383  | 0       |
-- | 351  | 9       |
-- | 228  | 18      |
-- +------+---------+

```

Find max, min, avg of n_fail
```sql
SELECT MAX(n_fail) AS max_n_fail, MIN(n_fail) AS min_n_fail, AVG(n_fail) AS avg_n_fail FROM boston_health;

-- +-------------+-------------+--------------------+
-- | max_n_fail  | min_n_fail  |     avg_n_fail     |
-- +-------------+-------------+--------------------+
-- | 7434        | 0           | 520.6087350489307  |
-- +-------------+-------------+--------------------+

```

Find the top 3 most occured n_fail, top 1 occured is the mode
```sql
SELECT COUNT(n_fail) AS num, n_fail FROM boston_health GROUP BY n_fail ORDER BY num DESC LIMIT 3;

-- +------+---------+
-- | num  | n_fail  |
-- +------+---------+
-- | 318  | 9       |
-- | 293  | 18      |
-- | 213  | 0       |
-- +------+---------+


```

Find max, min, avg of pass_rate
```sql
SELECT MAX(pass_rate) AS max_pass_rate, MIN(pass_rate) AS min_pass_rate, AVG(pass_rate) AS avg_pass_rate FROM boston_health;

-- +----------------+----------------+--------------------+
-- | max_pass_rate  | min_pass_rate  |   avg_pass_rate    |
-- +----------------+----------------+--------------------+
-- | 1.0            | 0.0            | 0.376200942370424  |
-- +----------------+----------------+--------------------+
```

Find the top 3 most occured pass_rate, top 1 occured is the mode
```sql
--  SELECT COUNT(pass_rate) AS num, pass_rate FROM boston_health GROUP BY pass_rate ORDER BY num DESC LIMIT 3;

-- +------+------------+
-- | num  | pass_rate  |
-- +------+------------+
-- | 383  | 0.0        |
-- | 318  | 0.5        |
-- | 213  | 1.0        |
-- +------+------------+

```

Find the max number of health pass for each city, in a descending order of max_n_pass
```sql
SELECT MAX(n_pass) as max_n_pass, city FROM boston_health GROUP BY city ORDER BY max_n_pass DESC;

-- +-------------+----------------+
-- | max_n_pass  |      city      |
-- +-------------+----------------+
-- | 2556        | Dorchester     |
-- | 2313        | East Boston    |
-- | 2313        | Boston         |
-- | 1881        | Roxbury        |
-- | 1872        | Roslindale     |
-- | 1791        | West Roxbury   |
-- | 1656        | Brighton       |
-- | 1584        | Chestnut Hill  |
-- | 1512        | Allston        |
-- | 1395        | Hyde Park      |
-- | 1359        | Charlestown    |
-- | 1287        | Jamaica Plain  |
-- | 1287        | Mattapan       |
-- | 1224        | South Boston   |
-- | 1170        | Mission Hill   |
-- | 414         | South End      |
-- | 72          |                |
-- +-------------+----------------+

```

Find the max number of health fail for each city, in a descending order of max_n_fail
```sql
SELECT MAX(n_fail) as max_n_fail, city FROM boston_health GROUP BY city ORDER BY max_n_fail DESC;

-- +-------------+----------------+
-- | max_n_fail  |      city      |
-- +-------------+----------------+
-- | 7434        | West Roxbury   |
-- | 6345        | Boston         |
-- | 5598        | Jamaica Plain  |
-- | 5184        | Roxbury        |
-- | 4536        | Dorchester     |
-- | 4167        | Mission Hill   |
-- | 4149        | Mattapan       |
-- | 4122        | Allston        |
-- | 4032        | East Boston    |
-- | 3978        | Hyde Park      |
-- | 3339        | Roslindale     |
-- | 3033        | South Boston   |
-- | 2601        | Brighton       |
-- | 2349        | Chestnut Hill  |
-- | 1926        | Charlestown    |
-- | 783         | South End      |
-- | 162         |                |
-- +-------------+----------------+
```

Find the max number of health pass_rate for each city, in a descending order of max_pass_rate
```sql
SELECT MAX(pass_rate) as max_pass_rate, city FROM boston_health GROUP BY city ORDER BY max_pass_rate DESC;
-- +----------------+----------------+
-- | max_pass_rate  |      city      |
-- +----------------+----------------+
-- | 1.0            | West Roxbury   |
-- | 1.0            | South Boston   |
-- | 1.0            | Roxbury        |
-- | 1.0            | Roslindale     |
-- | 1.0            | Jamaica Plain  |
-- | 1.0            | Hyde Park      |
-- | 1.0            | East Boston    |
-- | 1.0            | Dorchester     |
-- | 1.0            | Charlestown    |
-- | 1.0            | Brighton       |
-- | 1.0            | Boston         |
-- | 1.0            | Allston        |
-- | 0.875          | Mattapan       |
-- | 0.75           | Chestnut Hill  |
-- | 0.5            | Mission Hill   |
-- | 0.34586        | South End      |
-- | 0.30769        |                |
-- +----------------+----------------+

```


Find the min number of health pass for each city, in a ascending order of min_n_pass
```sql
SELECT MIN(n_pass) as min_n_pass, city FROM boston_health GROUP BY city ORDER BY min_n_pass;

-- +-------------+----------------+
-- | min_n_pass  |      city      |
-- +-------------+----------------+
-- | 0           | West Roxbury   |
-- | 0           | South Boston   |
-- | 0           | Roxbury        |
-- | 0           | Roslindale     |
-- | 0           | Mission Hill   |
-- | 0           | Mattapan       |
-- | 0           | Jamaica Plain  |
-- | 0           | Hyde Park      |
-- | 0           | East Boston    |
-- | 0           | Dorchester     |
-- | 0           | Charlestown    |
-- | 0           | Brighton       |
-- | 0           | Boston         |
-- | 0           | Allston        |
-- | 27          | Chestnut Hill  |
-- | 72          |                |
-- | 414         | South End      |
-- +-------------+----------------+

```

Find the min number of health fail for each city, in a ascending order of min_n_fail
```sql
SELECT MIN(n_fail) as min_n_fail, city FROM boston_health GROUP BY city ORDER BY min_n_fail;

-- +-------------+----------------+
-- | min_n_fail  |      city      |
-- +-------------+----------------+
-- | 0           | West Roxbury   |
-- | 0           | South Boston   |
-- | 0           | Roxbury        |
-- | 0           | Roslindale     |
-- | 0           | Jamaica Plain  |
-- | 0           | Hyde Park      |
-- | 0           | East Boston    |
-- | 0           | Dorchester     |
-- | 0           | Charlestown    |
-- | 0           | Brighton       |
-- | 0           | Boston         |
-- | 0           | Allston        |
-- | 9           | Mattapan       |
-- | 9           | Chestnut Hill  |
-- | 9           | Mission Hill   |
-- | 162         |                |
-- | 783         | South End      |
-- +-------------+----------------+

```

Find the min number of health pass_rate for each city, in a ascending order of min_pass_rate
```sql
SELECT MIN(pass_rate) as min_pass_rate, city FROM boston_health GROUP BY city ORDER BY min_pass_rate;

-- +----------------+----------------+
-- | min_pass_rate  |      city      |
-- +----------------+----------------+
-- | 0.0            | West Roxbury   |
-- | 0.0            | South Boston   |
-- | 0.0            | Roxbury        |
-- | 0.0            | Roslindale     |
-- | 0.0            | Mission Hill   |
-- | 0.0            | Mattapan       |
-- | 0.0            | Jamaica Plain  |
-- | 0.0            | Hyde Park      |
-- | 0.0            | East Boston    |
-- | 0.0            | Dorchester     |
-- | 0.0            | Charlestown    |
-- | 0.0            | Brighton       |
-- | 0.0            | Boston         |
-- | 0.0            | Allston        |
-- | 0.30769        |                |
-- | 0.34586        | South End      |
-- | 0.40275        | Chestnut Hill  |
-- +----------------+----------------+

```

Find the avg number of health pass for each city, in a descending order of avg_n_pass
```sql
SELECT AVG(n_pass) as avg_n_pass, city FROM boston_health GROUP BY city ORDER BY avg_n_pass DESC;

-- +---------------------+----------------+
-- |     avg_n_pass      |      city      |
-- +---------------------+----------------+
-- | 805.5               | Chestnut Hill  |
-- | 414.0               | South End      |
-- | 341.77868852459017  | West Roxbury   |
-- | 281.21311475409834  | Mission Hill   |
-- | 260.1024930747922   | East Boston    |
-- | 247.10969387755102  | Roxbury        |
-- | 238.01243980738363  | Boston         |
-- | 232.71428571428572  | Charlestown    |
-- | 228.6833976833977   | Jamaica Plain  |
-- | 227.628912071535    | Dorchester     |
-- | 225.27777777777777  | South Boston   |
-- | 220.21387283236993  | Roslindale     |
-- | 217.34117647058824  | Allston        |
-- | 205.125             | Mattapan       |
-- | 180.97297297297297  | Hyde Park      |
-- | 167.55172413793105  | Brighton       |
-- | 72.0                |                |
-- +---------------------+----------------+

```

Find the avg number of health fail for each city, in a descending order of avg_n_fail
```sql
SELECT AVG(n_fail) as avg_n_fail, city FROM boston_health GROUP BY city ORDER BY avg_n_fail DESC;

-- +---------------------+----------------+
-- |     avg_n_fail      |      city      |
-- +---------------------+----------------+
-- | 1179.0              | Chestnut Hill  |
-- | 791.6311475409836   | West Roxbury   |
-- | 783.0               | South End      |
-- | 729.7377049180328   | Mission Hill   |
-- | 725.1198979591836   | Roxbury        |
-- | 666.4169884169884   | Jamaica Plain  |
-- | 580.21875           | Mattapan       |
-- | 559.3681073025335   | Dorchester     |
-- | 525.9017341040462   | Roslindale     |
-- | 516.8333333333334   | South Boston   |
-- | 491.7062600321027   | Boston         |
-- | 473.1081081081081   | Hyde Park      |
-- | 436.1883656509695   | East Boston    |
-- | 410.15294117647056  | Allston        |
-- | 388.01020408163265  | Charlestown    |
-- | 336.41379310344826  | Brighton       |
-- | 162.0               |                |
-- +---------------------+----------------+

```

Find the avg number of health pass_rate for each city, in a descending order of avg_pass_rate
```sql
SELECT AVG(pass_rate) as avg_pass_rate, city FROM boston_health GROUP BY city ORDER BY avg_pass_rate DESC;

-- +----------------------+----------------+
-- |    avg_pass_rate     |      city      |
-- +----------------------+----------------+
-- | 0.576375             | Chestnut Hill  |
-- | 0.4549597959183674   | Charlestown    |
-- | 0.4253808033240998   | East Boston    |
-- | 0.4078645304975924   | Boston         |
-- | 0.3873342911877395   | Brighton       |
-- | 0.3695188524590164   | West Roxbury   |
-- | 0.35822109803921565  | Allston        |
-- | 0.3491301234567901   | South Boston   |
-- | 0.3466709009009009   | Hyde Park      |
-- | 0.34586              | South End      |
-- | 0.32957897168405376  | Dorchester     |
-- | 0.3265547398843931   | Roslindale     |
-- | 0.3138350965250965   | Jamaica Plain  |
-- | 0.30903520408163265  | Roxbury        |
-- | 0.30769              |                |
-- | 0.3004064583333333   | Mattapan       |
-- | 0.2960083606557377   | Mission Hill   |
-- +----------------------+----------------+

```


# STEP 3





## Profiling yelp_business

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





# STEP 4



## Profiling boston_stats


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