launchctl load /usr/local/Cellar/mosquitto/1.4.10_1/homebrew.mxcl.mosquitto.plist

launchctl unload /usr/local/Cellar/mosquitto/1.4.10_1/homebrew.mxcl.mosquitto.plist

killall mosquito

launchctl start homebrew.mxcl.mosquitto
mosquitto_pub -t "topic/path" -m "message payload6" -q 1 -r
 mosquitto_sub -t "topic/path"
launchctl stop homebrew.mxcl.mosquitto


export SPARK_HOME=/Users/romeokienzler/Documents/runtimes/spark-2.1.0-bin-hadoop2.7/
export PATH=$SPARK_HOME/bin:$PATH
//var washing = spark.read.format("json").load("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter2/washing.json")

//var washing_flat = washing.select("doc.*")
//washing_flat = washing_flat.repartition(1)
//washing_flat.write.json("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter6/washing_flat.json")

// spark-shell --packages org.apache.bahir:spark-sql-streaming-mqtt_2.11:2.1.0-SNAPSHOT,org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.1.0
// mvn dependency:resolve

import org.apache.spark.sql.types._
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


var inputStream = spark.readStream.format("json").schema(schema).load("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter6/washing_flat")

spark.conf.set("spark.sql.streaming.schemaInference",true)

var inputStream = spark.readStream.format("json").load("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter6/washing_flat")



val query = inputStream.writeStream.
  outputMode("append").
  format("console").
  start()

//TODO replace with MQTT stuff
var mqttStream = spark.readStream.format("json").load("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter6/washing_flat")

scala> mqttStream.groupBy(window($"ts", "10 minutes", "5 minutes"),$"temperature").avg()
org.apache.spark.sql.AnalysisException: cannot resolve 'timewindow(ts, 600000000, 300000000, 0)' due to data type mismatch: argument 1 requires timestamp type, however, '`ts`' is of bigint type.;;
TODO convert ts from long to Timestamp

val alertApplication = inputStream.writeStream.
  outputMode("append").
  format("console").
  start()

:paste
val lines = spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 9999)
  .load()



spark-shell --packages org.apache.bahir:spark-sql-streaming-mqtt_2.11:2.1.0-SNAPSHOT,org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.1.0


import org.apache.spark.sql.types._
val schema = StructType(
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

val mqtt_host = "pcoyha.messaging.internetofthings.ibmcloud.com"
val org = "pcoyha"
val apiKey = "a-pcoyha-oaigc1k8ub"
val apiToken = "&wuypVX2yNgVLAcLr8"
var randomSessionId = scala.util.Random.nextInt(10000)

:paste
val df = spark.readStream
    .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
    .option("username",apiKey)
    .option("password",apiToken)
    .option("clientId","a:"+org+":"+apiKey)
    .option("topic", "iot-2/type/WashingMachine/id/Washer01/evt/voltage/fmt/json")
    .load("tcp://"+mqtt_host+":1883")


val query = df.writeStream.
  outputMode("append").
  format("console").
  start()


val df = spark.readStream
    //.schema(schema)
    .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
    .option("username","a-vy0z2s-q6s8r693hv")
    .option("password","B+UX(a8GbddXWuFPvX")
    .option("clientId","a:vy0z2s:a-vy0z2s-zfzzckrnqf6")
    //.option("topic", "iot-2/type/TestDeviceType517/id/TestDevice517/evt/lorenz/fmt/json")
.option("topic", "iot-2/type/WashingMachine/id/Washer02/evt/fluid/fmt/json")
    .load("tcp://vy0z2s.messaging.internetofthings.ibmcloud.com:1883")



val df = spark.readStream
    .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
    .option("topic", "t/t")
    .load("tcp://localhost:1883")
    


val query = df.writeStream.
  outputMode("append").
  format("console").
  start()



val w = Window.partitionBy("timestamp").rowsBetween(-2, 2)
val counter = df.select(avg($"value").over(w))
--
:paste
import org.apache.spark.sql.expressions.Window
val df = spark.readStream
.format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")   
.option("topic", "topic/path")   .load("tcp://localhost:1883")

val counter = df.groupBy(
  window($"timestamp", "2 seconds"),
  $"value").agg(avg($"value"))
val query = counter.writeStream.
  outputMode("append").
  format("console").
  start()
query.awaitTermination()
https://www.youtube.com/watch?v=lUPXsVwvlD4
youtube-dl https://www.youtube.com/watch?v=_72cbZ5GwS4
youtube-dl https://www.youtube.com/watch?v=VtvP54Xo3Ek
youtube-dl https://www.youtube.com/watch?v=b1mTv9qfkNs
youtube-dl https://www.youtube.com/watch?v=pOyLAHwFiRk
youtube-dl https://www.youtube.com/watch?v=LrjKnGPXz14
youtube-dl https://www.youtube.com/watch?v=tXW8x-JS9HM
youtube-dl https://www.youtube.com/watch?v=al8cwKPx_8c