#NOTE: As stated in the README file in this directory, these codes are an earlier attempt at profiling with Mapreduce, and are not used in the final version of the project which uses Hive for cleaning and profiling. See subdirectory /zs1113 for those codes.


java -version 

#Compile Mapreduce files
yarn classpath 
javac -classpath `yarn classpath` -d . CountMapper.java
javac -classpath `yarn classpath` -d . CountReducer.java
javac -classpath `yarn classpath`:. -d . Count.java

jar -cvf Count.jar *.class

#Upload data onto hdfs
hdfs dfs -put BostonFood.csv hw9
hdfs dfs -ls hw9
#share directory access permission with Andrew
hdfs dfs -setfacl -R -m user:zs1113:rwx /user/sx663/hw9
//verify that it is shared 
hdfs dfs -getfacl /user/sx663/hw9
#Run the program
hadoop jar Count.jar Count hw9/BostonFood.csv /user/sx663/hw9/outputboston5

#Inspect &Download
hdfs dfs -cat hw9/outputboston5/part—r-00000
hdfs dfs -get /user/sx663/hw9/outputboston5/part-r-00000 /home/sx663
