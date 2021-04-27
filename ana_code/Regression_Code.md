
#REGRESSION ANALYSIS IN SPARK

#Rename the exported data and upload to hdfs
ls
hdfs dfs -rm -r -f sparkInput 
hdfs dfs -ls 
hdfs dfs -mkdir sparkInput
hdfs dfs -put FinalData.csv sparkInput
hdfs dfs -ls sparkInput



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


