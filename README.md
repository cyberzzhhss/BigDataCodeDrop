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
   

# File Directory
```
bbcd                                  
├─ ana_code                           
│  ├─ clean_data                      
│  │  └─ FinalData.csv                
│  ├─ ana_code.txt                    
│  └─ code_explanation.md             
├─ data_ingest                        
│  ├─ raw_data                        
│  │  ├─ boston_raw.csv               
│  │  └─ yelp_business.json           
│  ├─ data_ingest.txt                 
│  └─ data_ingest_explained.md        
├─ etl_code                           
│  ├─ sx663                           
│  │  ├─ Code.md                      
│  │  ├─ README_etl_sx663.md          
│  │  └─ etl_sx663.zip                
│  └─ zs1113                          
│     ├─ etl_code.txt                 
│     └─ etl_code_explained.md        
├─ profiling_code                     
│  ├─ sx663                           
│  │  ├─ Code.md                      
│  │  ├─ README_profiling_sx663.md    
│  │  └─ profiling_sx663.zip          
│  └─ zs1113                          
│     ├─ profiling_code.txt           
│     └─ profiling_code_explained.md  
├─ screenshots   
│  ├─ Regression1.png                       
│  ├─ Regression2.png                 
│  └─ Regression3.png                 
├─ README.md                          
└─ README_draft.md                    
```

# Instructions to run code


# Long Version (Preferablly using markdown editor Typora for colorful experience)

## Data Ingest
Open  BigDataCodeDrop/data_ingest/data_ingest_explained.md
Follow the instruction

## ETL Code
Open  BigDataCodeDrop/etl_code/zs1113/etl_code_explained.md
Follow the instruction

## Profiling Code
Open BigDataCodeDrop/profiling_code/zs1113/profiling_code_explained.md  
Follow the instruction

## Data Analytics
Open BigDataCodeDrop/ana_conde/code_explanation.md
Follow the instruction


# Short Version

The txt files only has raw code, no explanation, making it easier to copy and paste.
**Remember to replace [NetID] with your own.**

## Data Ingest
Open BigDataCodeDrop/data_ingest/data_ingest.txt

## ETL Code
Open  BigDataCodeDrop/etl_code/zs1113/etl_code.txt

## Profiling Code
Open BigDataCodeDrop/profiling_code/zs1113/profiling_code.txt

## Data Analytics
Open BigDataCodeDrop/ana_conde/ana_code.txt

# Proof of server access
Peel Server Access
```



```