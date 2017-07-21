case class Client(
    age: Long,
    countryCode: String,
    familyName: String,
    id: String,
    name: String
    )

val clientDs = spark.read.json("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/client.json").as[Client]

val clientDsBig = spark.read.json("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/client_big.json").as[Client]

case class Account(
    balance: Long,
    id: String,
    clientId: String
    )

val accountDs = spark.read.json("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/account.json").as[Account]

val accountDsBig = spark.read.json("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/account_big.json").as[Account]

clientDs.createOrReplaceTempView("client")
clientDsBig.createOrReplaceTempView("clientbig")

accountDs.createOrReplaceTempView("account")
accountDsBig.createOrReplaceTempView("accountbig")

//clientDs.write.parquet("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/client.parquet")

//clientDsBig.write.parquet("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/client_big.parquet")

//accountDs.write.parquet("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/account.parquet")

//accountDsBig.write.parquet("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/account_big.parquet")

//clientDsBig.write.csv("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/client_big.csv")

//accountDsBig.write.csv("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/account_big.csv")


val clientDsParquet = spark.read.parquet("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/client.parquet").as[Client]

val clientDsBigParquet = spark.read.parquet("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/client_big.parquet").as[Client]

val accountDsParquet = spark.read.parquet("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/account.parquet").as[Account]

val accountDsBigParquet = spark.read.parquet("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/account_big.parquet").as[Account]

val clientDsBigCsv = spark.read.csv("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/client_big.csv").as[Client]

clientDsParquet.createOrReplaceTempView("clientparquet")
clientDsBigParquet.createOrReplaceTempView("clientbigparquet")

accountDsParquet.createOrReplaceTempView("accountparquet")
accountDsBigParquet.createOrReplaceTempView("accountbigparquet")

spark.sql("select c.familyName from clientbigparquet c inner join accountbigparquet a on c.id=a.clientId").explain
== Physical Plan ==
*Project [familyName#79]
+- *BroadcastHashJoin [id#80], [clientId#106], Inner, BuildRight
   :- *Project [familyName#79, id#80]
   :  +- *Filter isnotnull(id#80)
   :     +- *BatchedScan parquet [familyName#79,id#80] Format: ParquetFormat, InputPaths: file:/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/client_b..., PushedFilters: [IsNotNull(id)], ReadSchema: struct<familyName:string,id:string>
   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]))
      +- *Project [clientId#106]
         +- *Filter isnotnull(clientId#106)
            +- *BatchedScan parquet [clientId#106] Format: ParquetFormat, InputPaths: file:/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/account_..., PushedFilters: [IsNotNull(clientId)], ReadSchema: struct<clientId:string>


spark.sql("select c.familyName from clientbig c inner join accountbig a on c.id=a.clientId").explain


spark.sql("select count(*) from clientbigparquet c inner join accountbigparquet a on c.id=a.clientId").explain
== Physical Plan ==
*HashAggregate(keys=[], functions=[count(1)])
+- Exchange SinglePartition
   +- *HashAggregate(keys=[], functions=[partial_count(1)])
      +- *Project
         +- *BroadcastHashJoin [id#199], [clientId#225], Inner, BuildRight
            :- *Project [id#199]
            :  +- *Filter isnotnull(id#199)
            :     +- *BatchedScan parquet [id#199] Format: ParquetFormat, InputPaths: file:/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/client_b..., PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:string>
            +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]))
               +- *Project [clientId#225]
                  +- *Filter isnotnull(clientId#225)
                     +- *BatchedScan parquet [clientId#225] Format: ParquetFormat, InputPaths: file:/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter3/account_..., PushedFilters: [IsNotNull(clientId)], ReadSchema: struct<clientId:string>

spark.sql("select count(*) from clientbig c inner join accountbigparquet a on c.id=a.clientId").explain


---------
private[sql] case class JDBCRelation(
    parts: Array[Partition], jdbcOptions: JDBCOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {
      
      
--
      
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    JDBCRDD.scanTable(
      sparkSession.sparkContext,
      schema,
      requiredColumns,
      filters,
      parts,
      jdbcOptions).asInstanceOf[RDD[Row]]
  }

--
      JDBCRDD.scala
      
"We'll skip the scanTable method for now since it just parameterizes and creates a 
new JDBCRDD object. So the most interesting method in JDBCRDD is compute which it inherrits from the abstract RDD class. Through the compute method ApacheSpark tells this RDD please go out of lazy mode and materialize yourself. We'll show you two important fractions of this methods after we had a look at the method signature TODO contract? "
      
  override def compute(thePart: Partition, context: TaskContext): Iterator[InternalRow] = {
    
"Here you can see that the return type is of Iterator which allows a lazy underlying data source to be read lazy as well. As we can see soon this is the case for this particular implementation as well"      

    val sqlText = s"SELECT $columnList FROM ${options.table} $myWhereClause"
    stmt = conn.prepareStatement(sqlText,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(options.fetchSize)
    rs = stmt.executeQuery()
      
"Note that the SQL statement created and stored in the sqlText constant is referencing two interesting variables. columnList and myWhereClause. Both are derived from the requiredColumns and filter arguments passed to the JDBCRelation class. Therefore this data source can be called a smart source because the underlying storage technology - a SQL data base in this case - can be told to only return columns and rows which are actually requested. And as already mentione, the data source supports passing lazy data access patterns to be pushed to the underlying data base as well. Here you can see that the JDBC result set is wrapped into a typed InternalRow iterator Iterator[InternalRow]]. Since this matches the return type of the comput method we are done here."
      
    val rowsIterator = JdbcUtils.resultSetToSparkInternalRows(rs, schema, inputMetrics)

    CompletionIterator[InternalRow, Iterator[InternalRow]](rowsIterator, close())
------------
      
Catalog
"Internally ApacheSpark uses the class org.apache.spark.sql.catalyst.catalog.SessionCatalog for managing temporary views as well as persistent tables. Temporary views are stored int the SparkSession object as persistent tables are stored in an external meta-store. The abstract base class org.apache.spark.sql.catalyst.catalog.ExternalCatalog is extended for varoius meta store providers. TODO One is for using ApacheDerby and another one is for the ApacheHive metastore but anyone could extend this class and make ApacheSpark to use another meta store as well."

----------
Unresolved plan
    
"An unresolved plan basically is the first tree created from either SQL statements or the relation API of DataFrames and Datasets. It is mainly composed of subtypes of LeafExpression object which are bound together by Expression object therefore forming a tree of TreeNode objects since all these objects are subtypes of TreeNode. Overall this data structure is a LogicalPlan which is therefor reflected as a LogicalPlan object. Note that the LogicalPlan extends QueryPlan and QueryPlan itself is a TreeNode again. In other words, a LogicalPlan is nothing else than a set of TreeNode objects    "  
      
      //TODO insert inheritance tree of treenode B05868_03_01.png B05868_03_02.png
      