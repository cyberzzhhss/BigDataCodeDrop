#NOTE: As stated in the README file in this directory, these codes are an earlier attempt at cleaning with Mapreduce (at that time we were thinking of using data for NYC, but later changed to Boston), and are not used in the final version of the project which uses Hive for cleaning and profiling. See subdirectory /zs1113 for those codes.


java -version 

#Compile Mapreduce files
yarn classpath 
javac -classpath `yarn classpath` -d . CleanMapper.java
javac -classpath `yarn classpath` -d . CleanReducer.java
javac -classpath `yarn classpath`:. -d . Clean.java

jar -cvf Clean.jar *.class

#Upload data onto hdfs
hdfs dfs -put DOHMH_New_York_City_Restaurant_Inspection_Results_Cleaned.csv hw7
hdfs dfs -ls hw7
#share directory access permission with Andrew
hdfs dfs -setfacl -R -m user:zs1113:rwx /user/sx663/hw7
//verify that it is shared hdfs dfs -getfacl /user/sx663/hw7
#Run the program
hadoop jar Clean.jar Clean hw7/DOHMH_New_York_City_Restaurant_Inspection_Results_Cleaned.csv /user/sx663/hw7/outputClean
#Inspect
hdfs dfs -cat hw7/outputClean/part—r-00000

