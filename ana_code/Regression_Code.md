## Preparation

The regression.scala has all the scala code, making it easier to run the code instead of copying paste the code one line at a time

### Rename the exported data and upload to hdfs
```shell
ls
hdfs dfs -rm -r -f sparkInput 
hdfs dfs -ls 
hdfs dfs -mkdir sparkInput
hdfs dfs -put FinalData.csv sparkInput
hdfs dfs -ls sparkInput
```
### Import package
```scala
import org.apache.spark.ml.feature.VectorAssembler
// import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.ml.regression.LinearRegression
// import org.apache.spark.ml.regression.LinearRegression

import org.apache.spark.ml.classification.LogisticRegression
// import org.apache.spark.ml.classification.LogisticRegression
```

### Import data table onto spark as dataframe

```scala
spark-shell --deploy-mode client
var df = spark.sql("SELECT * FROM csv.`/user/sx663/sparkInput/FinalData.csv`")
// df: org.apache.spark.sql.DataFrame = [_c0: string, _c1: string ... 10 more fields]
```
### Note:
```scala
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
```

### Extract specific columns for regression
```scala
var partone = df.selectExpr("cast(_c9 as double) _c9","cast(_c4 as double) _c4")
```

## First regression
* dependent variable: cleanliness(_c9 pass_rate); 
* independent variable: ratings(_c4 stars);

```scala
partone: org.apache.spark.sql.DataFrame = [_c9: double, _c4: double]
partone.printSchema()
//root
// |-- _c9: double (nullable = true)
// |-- _c4: double (nullable = true)
```

#### This is the independent variable "features"
```scala
var assemblerone = new VectorAssembler().setInputCols(Array("_c4")).setOutputCol("features")
// assemblerone: org.apache.spark.ml.feature.VectorAssembler = vecAssembler_c71fc44e20df
partone = assemblerone.setHandleInvalid("skip").transform(partone)
// partone: org.apache.spark.sql.DataFrame = [_c9: double, _c4: double ... 1 more field]
```
#### This is the dependent variable "label"
```scala
partone=partone.withColumnRenamed("_c9","label")
// partone: org.apache.spark.sql.DataFrame = [label: double, _c4: double ... 1 more field]
```
#### Perform Linear Regression
##### Declare regression
```scala
var lrone = new LinearRegression()
// lrone: org.apache.spark.ml.regression.LinearRegression = linReg_0a63623fd5c0
```
##### Fit data
```scala
var lrModelone = lrone.fit(partone)
// 21/04/23 14:50:52 WARN util.Instrumentation: [95bcd1fa] regParam is zero, which might cause numerical instability and overfitting.
// [Stage 2:>                                                          (0 + 1) / 1]21/04/23 14:50:53 WARN netlib.BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS
// 21/04/23 14:50:53 WARN netlib.BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeRefBLAS
// 21/04/23 14:50:53 WARN netlib.LAPACK: Failed to load implementation from: com.github.fommil.netlib.NativeSystemLAPACK
// 21/04/23 14:50:53 WARN netlib.LAPACK: Failed to load implementation from: com.github.fommil.netlib.NativeRefLAPACK
// lrModelone: org.apache.spark.ml.regression.LinearRegressionModel = linReg_0a63623fd5c0
```

#### Get Results/assess goodness of fit
##### Print out coefficients and intercepts
```scala
println(s"Coefficients: ${lrModelone.coefficients} Intercept: ${lrModelone.intercept}")
//Coefficients: [0.02988437905866312] Intercept: 0.2543787098282755
```

##### Evaluate regression
```scala
var summaryone = lrModelone.summary
// summaryone: org.apache.spark.ml.regression.LinearRegressionTrainingSummary = // org.apache.spark.ml.regression.LinearRegressionTrainingSummary@7cdbc04
```

```scala
println(s"r2: ${summaryone.r2}")
// r2: 0.0156945754049429
```

## Second regression: 
* dependent variable: cleanliness(_c9 pass_rate); 
* independent variable: online popularity(_c5 review counts);

```scala
var parttwo = df.selectExpr("cast(_c9 as double) _c9","cast(_c5 as integer) _c5")
// parttwo: org.apache.spark.sql.DataFrame = [_c9: double, _c5: int]
```

### Same procedure in setting up regression
```scala
var assemblertwo= new VectorAssembler().setInputCols(Array("_c5")).setOutputCol("features")
// assemblertwo: org.apache.spark.ml.feature.VectorAssembler = vecAssembler_ccd76671ec27
```


### Print out coefficients and intercepts
```scala
parttwo =assemblertwo.setHandleInvalid("skip").transform(parttwo)
// parttwo: org.apache.spark.sql.DataFrame = [_c9: double, _c5: int ... 1 more field]
```
```scala
parttwo=parttwo.withColumnRenamed("_c9","label")
// parttwo: org.apache.spark.sql.DataFrame = [label: double, _c5: int ... 1 more field]
```
```scala
var lrtwo = new LinearRegression()
// lrtwo: org.apache.spark.ml.regression.LinearRegression = linReg_c68a99948646
```
```scala
var lrModeltwo = lrtwo.fit(parttwo)
// 21/04/23 15:09:12 WARN util.Instrumentation: [51c53dae] regParam is zero, which might cause numerical instability and overfitting.
// lrModeltwo: org.apache.spark.ml.regression.LinearRegressionModel = linReg_c68a99948646
```
### Evaluate regression
```scala
println(s"Coefficients: ${lrModeltwo.coefficients} Intercept: ${lrModeltwo.intercept}")
// Coefficients: [-0.007075239993046872] Intercept: 5.386810461496709

var summarytwo = lrModeltwo.summary
// summarytwo: org.apache.spark.ml.regression.LinearRegressionTrainingSummary = // org.apache.spark.ml.regression.LinearRegressionTrainingSummary@1f2f7838

println(s"r2: ${summarytwo.r2}")
// r2: 0.00346864406594638
```

## Third regression: 
* independent variable: cleanliness(_c9 pass_rate); 
* dependent variable: whether in operation(_c6 is_open);


### Extract specific columns for regression
```scala
var part3= df.selectExpr("cast(_c6 as double) _c6","cast(_c9 as integer) _c9")
part3: org.apache.spark.sql.DataFrame = [_c6: double, _c9: int]
part3.printSchema()
// root
//  |-- _c6: double (nullable = true)
//  |-- _c9: integer (nullable = true)
```
### This is the independent variable "features"
```scala
var assembler3 = new VectorAssembler().setInputCols(Array("_c9")).setOutputCol("features")
// assembler3: org.apache.spark.ml.feature.VectorAssembler = vecAssembler_47ac0c21918d

part3 =assembler3.setHandleInvalid("skip").transform(part3)
// part3: org.apache.spark.sql.DataFrame = [_c6: double, _c9: int ... 1 more field]
```
#### This is the dependent variable "label"
```scala
part3 = part3.withColumnRenamed("_c6","label")
// part3: org.apache.spark.sql.DataFrame = [label: double, _c9: int ... 1 more field]
```

#### Declare logstic regression
```scala
var lr3 = new LogisticRegression()
// lr3: org.apache.spark.ml.classification.LogisticRegression = logreg_bd9bf4c6536d
```
#### Fit data
```scala
var lr3model = lr3.fit(part3)
// 21/04/27 22:51:21 WARN netlib.BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS
// 21/04/27 22:51:21 WARN netlib.BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeRefBLAS
// lr3model: org.apache.spark.ml.classification.LogisticRegressionModel = LogisticRegressionModel: uid = logreg_bd9bf4c6536d, numClasses = 2, numFeatures = 1
```

#### Get results
```scala
println(s"Coefficients: ${lr3model.coefficients} Intercept: ${lr3model.intercept}")
// Coefficients: [0.05366519262928552] Intercept: 0.5653731412215562
```
