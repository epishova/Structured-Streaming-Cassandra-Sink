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

  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    println(s"Open connection")
    true
  }

  def process(record: org.apache.spark.sql.Row) = {
    //log.warn(s"Saving record: $record")
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

object SparkSessionBuilder {
  def buildSparkSession() = {
    val conf = new SparkConf()
    .setAppName("Structured Streaming from Kafka to Cassandra")
    .setMaster("local[2]")
    .set("spark.cassandra.connection.host", "ec2-52-23-103-178.compute-1.amazonaws.com")
    .set("spark.sql.streaming.checkpointLocation", "checkpoint")
    //.set("es.nodes", "localhost")
    //.set("es.index.auto.create", "true")
    //.set("es.nodes.wan.only", "true")
    .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    SparkSession
    .builder()
    .getOrCreate()
  }
}

object CassandraDriver {
  //val conf = new SparkConf()
  //.setAppName("Structured Streaming from Kafka to Cassandra")
  //.setMaster("local[2]")
  //.set("spark.cassandra.connection.host", "ec2-52-23-103-178.compute-1.amazonaws.com")
  //.set("spark.sql.streaming.checkpointLocation", "checkpoint")
  //.set("es.nodes", "localhost")
  //.set("es.index.auto.create", "true")
  //.set("es.nodes.wan.only", "true")
  //.set("spark.driver.allowMultipleContexts", "true")

  //val sc = new SparkContext(conf)
  //sc.setLogLevel("WARN")

  //private val spark = SparkSession
  //.builder()
  //.getOrCreate()
  private val spark = SparkSessionBuilder.buildSparkSession()
  
  import spark.implicits._

  val connector = CassandraConnector(spark.sparkContext.getConf)

  val namespace = "fx"
  val foreachTableSink = "spark_struct_stream_sink"
}

object KafkaToCassandra {
  def main(args: Array[String]) {
    //val conf = new SparkConf()
    //.setAppName("Structured Streaming from Kafka to Cassandra")
    //.setMaster("local[2]")
    //.set("spark.cassandra.connection.host", "ec2-52-23-103-178.compute-1.amazonaws.com")
    //.set("spark.sql.streaming.checkpointLocation", "checkpoint")
    //.set("es.nodes", "localhost")
    //.set("es.index.auto.create", "true")
    //.set("es.nodes.wan.only", "true")
    //.set("spark.driver.allowMultipleContexts", "true")

    //val sc = new SparkContext(conf)
    //sc.setLogLevel("WARN")

    //val spark = SparkSession
    //.builder()
    //.getOrCreate()
    val spark = SparkSessionBuilder.buildSparkSession()
  
    import spark.implicits._

    val broker = "ec2-18-209-75-68.compute-1.amazonaws.com:9092,ec2-18-205-142-57.compute-1.amazonaws.com:9092,ec2-50-17-32-144.compute-1.amazonaws.com:9092"

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
