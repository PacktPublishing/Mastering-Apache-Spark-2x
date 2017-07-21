val schema = StructType(
    StructField("_id",StringType,true)::
    StructField("_rev",StringType,true)::
    StructField("count",LongType,true)::
    StructField("flowrate",LongType,true)::
    StructField("fluidlevel",StringType,true)::
    StructField("frequency",LongType,true)::
    StructField("hardness",LongType,true)::
    StructField("speed",LongType,true)::
    StructField("temperature",LongType,true)::
    StructField("ts",LongType,true)::
    StructField("voltage",LongType,true)::
Nil)

val rowRDD = rawRDD.
    map(_.split(",")).
    map(p => Row(
            p(0),
            p(1),
            p(2).trim.toLong,
            p(3).trim.toLong,
            p(4),
            p(5).trim.toLong,
            p(6).trim.toLong,
            p(7).trim.toLong,
            p(8).trim.toLong,
            p(9).trim.toLong,
            p(10).trim.toLong
        )
    )
    
val washing_flat_df = spark.createDataFrame(rowRDD, schema)

val result = spark.sql("""
SELECT * from (
    SELECT
    min(temperature) over w as min_temperature,
    max(temperature) over w as max_temperature, 
    min(voltage) over w as min_voltage,
    max(voltage) over w as max_voltage,
    min(flowrate) over w as min_flowrate,
    max(flowrate) over w as max_flowrate,
    min(frequency) over w as min_frequency,
    max(frequency) over w as max_frequency,
    min(hardness) over w as min_hardness,
    max(hardness) over w as max_hardness,
    min(speed) over w as min_speed,
    max(speed) over w as max_speed
    FROM washing 
    WINDOW w AS (ORDER BY ts ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING) 
)
WHERE min_temperature is not null 
AND max_temperature is not null
AND min_voltage is not null
AND max_voltage is not null
AND min_flowrate is not null
AND max_flowrate is not null
AND min_frequency is not null
AND max_frequency is not null
AND min_hardness is not null
AND min_speed is not null
AND max_speed is not null   
""")

---
scala> var client = spark.read.json("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter2/client.json")
client: org.apache.spark.sql.DataFrame = [countryCode: string, familyName: string ... 2 more fields]

scala> var account = spark.read.json("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter2/account.json")
account: org.apache.spark.sql.DataFrame = [countryCode: string, familyName: string ... 2 more fields]


client.createOrReplaceTempView("client")
account.createOrReplaceTempView("account")

spark.sql("select * from client").show
spark.sql("select * from account").show

spark.sql("select * from account inner join client on account.clientid = client.id").show

spark.sql("select sum(balance),clientId from account inner join client on account.clientid = client.id group by clientId").show
---
object AgeRange extends Enumeration {
  val Zero, Ten, Twenty, Thirty, Fourty, Fifty, Sixty, Seventy, Eighty, Ninety, HundretPlus = Value
  def getAgeRange(age: Integer) = {
    age match {
      case age if 0 until 10 contains age => Zero
      case age if 11 until 20 contains age => Ten
      case age if 21 until 30 contains age => Twenty
      case age if 31 until 40 contains age => Thirty
      case age if 41 until 50 contains age => Fourty
      case age if 51 until 60 contains age => Fifty
      case age if 61 until 70 contains age => Sixty
      case age if 71 until 80 contains age => Seventy
      case age if 81 until 90 contains age => Eighty
      case age if 91 until 100 contains age => Ninety
      case _ => HundretPlus
    }
  }
  def asString(age: Integer) = getAgeRange(age).toString
}

scala> spark.udf.register("toAgeRange",AgeRange.asString _)
res12: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,StringType,Some(List(IntegerType)))

spark.sql("select *,toAgeRange(age) as ageRange from client").show
+---+-----------+-----------+---+---------+--------+
|age|countryCode| familyName| id|     name|ageRange|
+---+-----------+-----------+---+---------+--------+
| 33|         US|familyName1|  1|testName1|  Thirty|
| 43|         DE|familyName2|  2|testName2|  Fourty|
| 53|         US|familyName3|  3|testName3|   Fifty|
| 63|         CH|familyName4|  4|testName4|   Sixty|
| 73|         US|familyName5|  5|testName5| Seventy|
| 23|         DE|familyName6|  6|testName6|  Twenty|
| 36|         US|familyName7|  7|testName7|  Thirty|
| 38|         CH|familyName8|  8|testName8|  Thirty|
+---+-----------+-----------+---+---------+--------+
---
case class Client(
    age: Long,
    countryCode: String,
    familyName: String,
    id: String,
    name: String
    )

val ds = spark.read.json("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter2/client.json").as[Client]

scala> ds.show
+---+-----------+-----------+---+---------+
|age|countryCode| familyName| id|     name|
+---+-----------+-----------+---+---------+
| 33|         US|familyName1|  1|testName1|
| 43|         DE|familyName2|  2|testName2|
| 53|         US|familyName3|  3|testName3|
| 63|         CH|familyName4|  4|testName4|
| 73|         US|familyName5|  5|testName5|
| 23|         DE|familyName6|  6|testName6|
| 36|         US|familyName7|  7|testName7|
| 38|         CH|familyName8|  8|testName8|
+---+-----------+-----------+---+---------+


scala> ds.printSchema
root
 |-- age: long (nullable = true)
 |-- countryCode: string (nullable = true)
 |-- familyName: string (nullable = true)
 |-- id: string (nullable = true)
 |-- name: string (nullable = true)

val dsNew = ds.filter(e => {e.age >= 18}).
    map(e => (e.age, e.countryCode)).
    groupBy($"_2").
    avg()

scala> dsNew.show
+---+-------+
| _2|avg(_1)|
+---+-------+
| DE|   33.0|
| US|  48.75|
| CH|   50.5|
+---+-------+


---

scala> import hive._
import hive._


hiveContext.sql("""
CREATE TABLE IF NOT EXISTS adult2
            (
              idx             INT,
              age             INT,
              workclass       STRING,
              fnlwgt          INT,
              education       STRING,
              educationnum    INT,
              maritalstatus   STRING,
              occupation      STRING,
              relationship    STRING,
              race            STRING,
              gender          STRING,
              capitalgain     INT,
              capitalloss     INT,
              nativecountry   STRING,
              income          STRING
            )
            
""")


scala> hiveContext.sql("""
     | CREATE TABLE IF NOT EXISTS adult2
     |             (
     |               idx             INT,
     |               age             INT,
     |               workclass       STRING,
     |               fnlwgt          INT,
     |               education       STRING,
     |               educationnum    INT,
     |               maritalstatus   STRING,
     |               occupation      STRING,
     |               relationship    STRING,
     |               race            STRING,
     |               gender          STRING,
     |               capitalgain     INT,
     |               capitalloss     INT,
     |               nativecountry   STRING,
     |               income          STRING
     |             )
     |             
     | """)
res42: org.apache.spark.sql.DataFrame = []

var df = hiveContext.sql("SELECT COUNT(*) from adult2")
df.show
var df = hiveContext.sql("SELECT * from adult2")
df.show

hiveContext.sql("CREATE INDEX IX on adult2 using idx")