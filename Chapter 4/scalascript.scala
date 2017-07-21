//create 10^6 integers and store it in intList
val intList = 0 to math.pow(10, 6).toInt

//create a RDD out of intList
val rdd = sc.parallelize(intList)

//..and count it
rdd.cache.count

//create a DataFrame out of intList
val df = intList.toDF

//..and count it
df.cache.count

//create a Dataset out of DataFrame df
case class Ints(value: Int)
val ds = df.as[Ints]

//..and count it
ds.cache.count

output:
--------------------------------------
scala> //create 10^6 integers and store it in intList

scala> val intList = 0 to math.pow(10, 6).toInt
intList: scala.collection.immutable.Range.Inclusive = Range(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 16...
scala> 

scala> //create a RDD out of intList

scala> val rdd = sc.parallelize(intList)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:26

scala> 

scala> //..and count it

scala> rdd.cache.count
res0: Long = 1000001                                                            

scala> 

scala> //create a DataFrame out of intList

scala> val df = intList.toDF
df: org.apache.spark.sql.DataFrame = [value: int]

scala> 

scala> //..and count it

scala> df.cache.count
17/01/04 07:04:37 WARN TaskSetManager: Stage 1 contains a task of very large size (4033 KB). The maximum recommended task size is 100 KB.
res1: Long = 1000001                                                            

scala> 

scala> //create a Dataset out of DataFrame df

scala> case class Ints(value: Int)
defined class Ints

scala> val ds = df.as[Ints]
ds: org.apache.spark.sql.Dataset[Ints] = [value: int]

scala> 

scala> //..and count it

scala> ds.cache.count
17/01/04 07:04:40 WARN CacheManager: Asked to cache already cached data.
17/01/04 07:04:44 WARN TaskSetManager: Stage 3 contains a task of very large size (4033 KB). The maximum recommended task size is 100 KB.
res2: Long = 1000001                                                            
--------------------------------------
                                                            
                                                            
Spark V2 code generation
                                                            
scala> spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(),"id").explain()
== Physical Plan ==
*Project [id#38L]
+- *BroadcastHashJoin [id#38L], [id#41L], Inner, BuildRight
   :- *Range (0, 1000000000, splits=8)
   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]))
      +- *Range (0, 1000, splits=8)
                                            
                                                            
scala> spark.conf.set("spark.sql.codegen.wholeStage",false)

scala> spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(),"id").explain()
== Physical Plan ==
Project [id#49L]
+- BroadcastHashJoin [id#49L], [id#52L], Inner, BuildRight
   :- Range (0, 1000000000, splits=8)
   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]))
      +- Range (0, 1000, splits=8)
                                                    
scala> spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(),"id").count()
res12: Long = 1000
                                                            
                                                            