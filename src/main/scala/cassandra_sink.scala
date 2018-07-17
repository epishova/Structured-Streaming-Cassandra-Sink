package com.insight.app.CassandraSink

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.expr

class CassandraSinkForeach() extends ForeachWriter[org.apache.spark.sql.Row] {
  // This class implements the interface ForeachWriter, which has methods that get called 
  // whenever there is a sequence of rows generated as output
  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    println(s"Open connection")
    true
  }

  def process(record: org.apache.spark.sql.Row) = {
    println(s"Process new $record")
    CassandraDriver.connector.withSessionDo(session =>
      session.execute(s"""
       insert into ${CassandraDriver.namespace}.${CassandraDriver.foreachTableSink} (fx_marker, timestamp_ms, timestamp_dt)
       values('${record(0)}', '${record(1)}', '${record(2)}')""")
    )
  }

  def close(errorOrNull: Throwable): Unit = {
    // close the connection
    println(s"Close connection")
  }
}

class SparkSessionBuilder extends Serializable {
  // Build a spark session. Class is made serializable so to get access to SparkSession in a driver and executors. 
  // Note here the usage of @transient lazy val 
  def buildSparkSession: SparkSession = {
    @transient lazy val conf: SparkConf = new SparkConf()
    .setAppName("Structured Streaming from Kafka to Cassandra")
    .set("spark.cassandra.connection.host", "ec2-52-23-103-178.compute-1.amazonaws.com")
    .set("spark.sql.streaming.checkpointLocation", "checkpoint")

    @transient lazy val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

    spark
  }
}

object CassandraDriver extends SparkSessionBuilder {
  // This object will be used in CassandraSinkForeach to connect to Cassandra DB from an executor.
  // It extends SparkSessionBuilder so to use the same SparkSession on each node.
  val spark = buildSparkSession
  
  import spark.implicits._

  val connector = CassandraConnector(spark.sparkContext.getConf)
  
  // Define Cassandra's table which will be used as a sink
  /* For this app I used the following table:
       CREATE TABLE fx.spark_struct_stream_sink (
       fx_marker text,
       timestamp_ms timestamp,
       timestamp_dt date,
       primary key (fx_marker));
  */
  val namespace = "fx"
  val foreachTableSink = "spark_struct_stream_sink"
}

object KafkaToCassandra extends SparkSessionBuilder {
  // Main body of the app. It also extends SparkSessionBuilder.
  def main(args: Array[String]) {
    val spark = buildSparkSession
  
    import spark.implicits._
    
    // Define location of Kafka brokers:
    val broker = "ec2-18-209-75-68.compute-1.amazonaws.com:9092,ec2-18-205-142-57.compute-1.amazonaws.com:9092,ec2-50-17-32-144.compute-1.amazonaws.com:9092"

    /*Here is an example massage which I get from a Kafka stream. It contains multiple jsons separated by \n 
    {"timestamp_ms": "1530305100936", "fx_marker": "EUR/GBP"}
    {"timestamp_ms": "1530305100815", "fx_marker": "USD/CHF"}
    {"timestamp_ms": "1530305100969", "fx_marker": "EUR/CHF"}
    {"timestamp_ms": "1530305100011", "fx_marker": "USD/CAD"}
    */
    val dfraw = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("subscribe", "currency_exchange")
    .load()

    val schema = StructType(
      Seq(
        StructField("fx_marker", StringType, false),
        StructField("timestamp_ms", StringType, false)
      )
    )

    val df = dfraw
    .selectExpr("CAST(value AS STRING)").as[String]
    .flatMap(_.split("\n"))

    val jsons = df.select(from_json($"value", schema) as "data").select("data.*")

    val parsed = jsons
      .withColumn("timestamp_dt", to_date(from_unixtime($"timestamp_ms"/1000.0, "yyyy-MM-dd HH:mm:ss.SSS")))
      .filter("fx_marker != ''")

    val sink = parsed
    .writeStream
    .queryName("KafkaToCassandraForeach")
    .outputMode("update")
    .foreach(new CassandraSinkForeach())
    .start()

    sink.awaitTermination()
  }
}
