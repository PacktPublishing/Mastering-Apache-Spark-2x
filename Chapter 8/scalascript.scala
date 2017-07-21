:paste
//spark-shell --conf spark.executor.memory=10g
val basePath = "/Users/romeokienzler/Downloads/bosch/"
/**
def convert(filePrefix : String) = {
    var df = spark.read.option("header",true).option("inferSchema", "true").csv(basePath+filePrefix+".csv")
    df = df.repartition(1)
df.write.parquet(basePath+filePrefix+".parquet")
}

convert("train_numeric")
convert("train_date")
convert("train_categorical")
*/
var df_numeric = spark.read.parquet(basePath+"train_numeric.parquet")

var df_date = spark.read.parquet(basePath+"train_date.parquet")

var df_categorical = spark.read.parquet(basePath+"train_categorical.parquet")

df_categorical.createOrReplaceTempView("dfcat")
var dfcat = spark.sql("select Id, L0_S22_F545 from dfcat")
/**
dfcat.select("L0_S22_F545").distinct.show
+-----------+
|L0_S22_F545|
+-----------+
|        T16|
|  T12582912|
|       null|
|     T48576|
|  T16777232|
|       T512|
|    T589824|
|      T1372|
|         T8|
|  T16777557|
|        T32|
|      T6553|
| T-18748192|
|        T96|
+-----------+

*/

df_numeric.createOrReplaceTempView("dfnum")
var dfnum = spark.sql("select Id,L0_S0_F0,L0_S0_F2,L0_S0_F4,Response from dfnum")
/**
+---+--------+--------+--------+
| Id|L0_S0_F0|L0_S0_F2|L0_S0_F4|
+---+--------+--------+--------+
|  4|    0.03|  -0.034|  -0.197|
+---+--------+--------+--------+
*/

var df = dfcat.join(dfnum,"Id")
df.createOrReplaceTempView("df")

var df_notnull = spark.sql("""
select
    Response as label,
    case 
       when L0_S22_F545 is null then 'NA' 
       else L0_S22_F545 end as L0_S22_F545, 
    case
       when L0_S0_F0 is null then 0.0 
       else L0_S0_F0 end as L0_S0_F0, 
    case
       when L0_S0_F2 is null then 0.0 
       else L0_S0_F2 end as L0_S0_F2,
    case
       when L0_S0_F4 is null then 0.0 
       else L0_S0_F4 end as L0_S0_F4
from df
""")


import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

var indexer = new StringIndexer()
  .setHandleInvalid("skip")
  .setInputCol("L0_S22_F545")
  .setOutputCol("L0_S22_F545Index")

var indexed = indexer.fit(df_notnull).transform(df_notnull)
indexed.printSchema
indexed.select("L0_S22_F545","L0_S22_F545Index").distinct.show

/**
+----------------+                                                              
|L0_S22_F545Index|
+----------------+
|             8.0|
|             0.0|
|             7.0|
|             1.0|
|             4.0|
|            11.0|
|             3.0|
|             2.0|
|            10.0|
|            13.0|
|             6.0|
|             5.0|
|             9.0|
|            12.0|
+----------------+
*/

var encoder = new OneHotEncoder()
  .setInputCol("L0_S22_F545Index")
  .setOutputCol("L0_S22_F545Vec")

var encoded = encoder.transform(indexed)
//encoded.show()
/**
+-----------+----------------+--------------+
|L0_S22_F545|L0_S22_F545Index|L0_S22_F545Vec|
+-----------+----------------+--------------+
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
|         NA|             0.0|(13,[0],[1.0])|
+-----------+----------------+--------------+

encoded.select("L0_S22_F545Index","L0_S22_F545Vec").distinct.show
+----------------+---------------+                                              
|L0_S22_F545Index| L0_S22_F545Vec|
+----------------+---------------+
|            11.0|(13,[11],[1.0])|
|             2.0| (13,[2],[1.0])|
|             8.0| (13,[8],[1.0])|
|             3.0| (13,[3],[1.0])|
|            10.0|(13,[10],[1.0])|
|             6.0| (13,[6],[1.0])|
|             7.0| (13,[7],[1.0])|
|            12.0|(13,[12],[1.0])|
|             9.0| (13,[9],[1.0])|
|             4.0| (13,[4],[1.0])|
|             5.0| (13,[5],[1.0])|
|             0.0| (13,[0],[1.0])|
|             1.0| (13,[1],[1.0])|
|            13.0|     (13,[],[])|
+----------------+---------------+
*/

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

var vectorAssembler = new VectorAssembler()
        .setInputCols(Array("L0_S22_F545Vec", "L0_S0_F0", "L0_S0_F2","L0_S0_F4"))
        .setOutputCol("features")

var assembled = vectorAssembler.transform(encoded)

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel

//Create an array out of individual pipeline stages
var transformers = Array(indexer,encoder,vectorAssembler)


var pipeline = new Pipeline().setStages(transformers).fit(df_notnull)

var transformed = pipeline.transform(df_notnull)

import org.apache.spark.ml.classification.RandomForestClassifier
var rf = new RandomForestClassifier() 
  .setLabelCol("label")
  .setFeaturesCol("features")

var model = new Pipeline().setStages(pipeline :+ rf)).fit(df_notnull)

var result = model.transform(df_notnull)

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
val evaluator = new BinaryClassificationEvaluator()

import org.apache.spark.ml.param.ParamMap
var evaluatorParamMap = ParamMap(evaluator.metricName -> "areaUnderROC")
var aucTraining = evaluator.evaluate(result, evaluatorParamMap)


import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
/**var paramGrid = new ParamGridBuilder()
    .addGrid(rf.numTrees, 3 :: 5 :: 10 :: 30 :: 50 :: 70 :: 100 :: 150 :: Nil)
    .addGrid(rf.featureSubsetStrategy, "auto" :: "all" :: "sqrt" :: "log2" :: "onethird" :: Nil)
    .addGrid(rf.impurity, "gini" :: "entropy" :: Nil)    
    .addGrid(rf.maxBins, 2 :: 5 :: 10 :: 15 :: 20 :: 25 :: 30 :: Nil)
    .addGrid(rf.maxDepth, 3 :: 5 :: 10 :: 15 :: 20 :: 25 :: 30 :: Nil)
    .build()*/

var paramGrid = new ParamGridBuilder()
    .addGrid(rf.numTrees, 3 :: 5 :: 10 :: Nil)
    .addGrid(rf.featureSubsetStrategy, "auto" :: "all" :: Nil)
    .addGrid(rf.impurity, "gini" :: "entropy" :: Nil)    
    .addGrid(rf.maxBins, 2 :: 5 :: Nil)
    .addGrid(rf.maxDepth, 3 :: 5 :: Nil)
    .build()

var crossValidator = new CrossValidator()
      .setEstimator(new Pipeline().setStages(transformers :+ rf))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)
.setEvaluator(evaluator)


//Model is created
    var crossValidatorModel = crossValidator.fit(df_notnull)
    //Model used to Predict
    var newPredictions = crossValidatorModel.transform(df_notnull)


    var newAucTest = evaluator.evaluate(newPredictions, evaluatorParamMap)
    println("new AUC (with Cross Validation) " + newAucTest)
    var bestModel = crossValidatorModel.bestModel

    //Understand the Model selected
    println()
    println("Parameters for Best Model:")

    var bestPipelineModel = crossValidatorModel.bestModel.asInstanceOf[PipelineModel]
    var stages = bestPipelineModel.stages



import org.apache.spark.ml.classification.RandomForestClassificationModel
    val rfStage = stages(stages.length-1).asInstanceOf[RandomForestClassificationModel]
rfStage.getNumTrees
rfStage.getFeatureSubsetStrategy
rfStage.getImpurity
rfStage.getMaxBins
rfStage.getMaxDepth




//df = df.repartition(1)
//df.write.parquet("/Users/romeokienzler/Downloads/bosch/train_join.parquet")
