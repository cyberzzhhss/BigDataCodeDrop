# Source for Datasets:

1. The Food Violation record dataset (**rename** the file as **"boston_raw.csv"** before uploading to hdfs):
   
   The file updated on a daily basis. Our file version is on April 21.
   Your attempt to replicate the result might be different if you choose a dataset from a different date.
   The raw file used for our project is from **April 21** and is **stored as boston_raw.csv inside the peel server**.

   https://data.boston.gov/dataset/food-establishment-inspections/resource/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c

2. The yelp dataset (rename "yelp_business.json"):
   https://www.yelp.com/dataset/download

   * Download from the JSON section
   * After downloading, unzip **yelp_dataset.tar**
   * Go inside yelp_dataset folder
   * yelp_academic_dataset_business.json 
     * should be modified at this date: January 28, 2021 at 2:06 PM
     * should be 124.4 MB large
   * **rename** **"yelp_academic_dataset_business.json"** as **"yelp_business.json"**


Instead of copying and pasting the code one line at a time.
The raw code is inside data_digest.txt
Remember to replace [NetID] with your own.

# Dataset 1

Commands 
```shell
ls
hdfs dfs -rm -r -f hiveInput 
hdfs dfs -ls 
hdfs dfs -mkdir hiveInput
hdfs dfs -put boston_raw.csv hiveInput
hdfs dfs -ls hiveInput
```

Log into hive
```shell
beeline --silent
!connect jdbc:hive2://hm-1.hpc.nyu.edu:10000/
[NetID]
[PassCode]
use [NetID];
```

Create table boston_raw
```
DROP TABLE IF EXISTS boston_raw;

CREATE EXTERNAL TABLE boston_raw (businessname STRING,dbaname STRING,legalowner STRING,namelast STRING,namefirst STRING,licenseno INT,issdttm STRING,expdttm STRING,licstatus STRING,licensecat STRING,descript STRING,result STRING,resultdttm STRING,violation STRING,viollevel STRING,violdesc STRING,violdttm STRING,violstatus STRING,statusdate STRING,comments STRING,address STRING,city STRING,state STRING,zip STRING,property_id INT, latitude STRING, longitude STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/[NetID]/boston_raw'  tblproperties("skip.header.line.count"="1"); 

LOAD DATA INPATH '/user/[NetID]/hiveInput/boston_raw.csv' INTO TABLE boston_raw;
```
# Dataset 2


Commands
```
ls
hdfs dfs -rm -r -f hiveInput 
hdfs dfs -ls 
hdfs dfs -mkdir hiveInput
hdfs dfs -put yelp_business.json hiveInput
hdfs dfs -ls hiveInput
```

Log into hive
```
beeline --silent
!connect jdbc:hive2://hm-1.hpc.nyu.edu:10000/
[NetID]
[PassCode]
use [NetID];
```

Create table json_tab
```
DROP TABLE IF EXISTS json_tab;
CREATE TABLE json_tab(col1 string);
LOAD DATA INPATH '/user/[NetID]/hiveInput/yelp_business.json' INTO TABLE json_tab;
```

