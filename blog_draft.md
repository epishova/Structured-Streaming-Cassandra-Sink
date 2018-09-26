# Cassandra Sink for Spark Structured Streaming

I've recently begun to use Spark, and at some point had to store the results produced by Structured Streaming API in Cassandra database.
In this blog I provide a simple example of how to create and use Cassandra sink in Spark Structured Streaming. I hope it will be 
useful for those who have just begun to work with Structured Streaming API and wonder how to connect it with a database.

## Why Structured Streaming?

From [documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) Structured Streaming is a 
scalable and fault-tolerant stream processing engine built on the Spark SQL engine. You can use the Dataset/DataFrame API to express 
streaming aggregations, event-time windows, stream-to-batch joins, etc. They key idea is that Structured Streaming unifies batch and 
streaming computation in Spark and allows processing streaming data by using familiar SQL.

## What is the problem?

Spark Structured Streaming has been marked stable in 2017. So, it is relatively new API and not all features are there yet. For example,
there are a few types of built-in output sinks: file, kafka, console, and memory sinks. However, if you’d like to output the results 
of your streaming computation into a database you  would need to use the `foreach` sink and implement the interface `ForeachWriter`. **As of Spark 2.3.1, this is available only for Scala and Java**.

Here, I assume that you've already figured out how Structured Streaming works in outline, and know how to read and process your 
streaming data, and now are ready to output it into a database. If some of the above steps are unclear, there is a number of 
good online resources which can help you to start working with Structured Streaming. In particular, official documentation is 
a good place to start. In this article, I would like to focus on the last step when you need to store the results in a database.

I will describe how to implement Cassandra sink for Structured Streaming, provide a simple example, and explain how to run it on 
a cluster. The full code is available [here](https://github.com/epishova/Structured-Streaming-Cassandra-Sink). 

When I initially faced the above problem [this project](https://github.com/polomarcus/Spark-Structured-Streaming-Examples) was very 
helpful. However, that repo may seem complex if you just began to work with Structured Streaming and need a simple example of how 
to output data into Cassandra. Furthermore, that code works in Spark local mode and requires some changes to run on a cluster.

Additionally, there are good examples of how to create [JDBC sink](https://docs.databricks.com/_static/notebooks/structured-streaming-etl-kafka.html) and [MongoDB sink](https://jira.mongodb.org/browse/SPARK-134) 
for Structured Streaming.

## Simple solution

To send data to external systems you need to use `foreach` sink. You can read more about it 
[here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach). In short, you need to implement 
interface `ForeachWriter`. That is to define how to open connection, to process each partition of data, and to close connection at 
the end of processing. The source code looks as follows:

```scala
class CassandraSinkForeach() extends ForeachWriter[org.apache.spark.sql.Row] {
  // This class implements the interface ForeachWriter, which has methods that get called 
  // whenever there is a sequence of rows generated as output

  val cassandraDriver = new CassandraDriver();
  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    println(s"Open connection")
    true
  }

  def process(record: org.apache.spark.sql.Row) = {
    println(s"Process new $record")
    cassandraDriver.connector.withSessionDo(session =>
      session.execute(s"""
       insert into ${cassandraDriver.namespace}.${cassandraDriver.foreachTableSink} (fx_marker, timestamp_ms, timestamp_dt)
       values('${record(0)}', '${record(1)}', '${record(2)}')""")
    )
  }

  def close(errorOrNull: Throwable): Unit = {
    // close the connection
    println(s"Close connection")
  }
}
```

You will find the definition of `CassandraDriver` and the output table below but before that let's have a closer look on how the above code works. Here, to connect to Cassandra from Spark, I create `CassandraDriver` object which provides access to `CassandraConnector` – a widely used connector from DataStax. You can read more about it [here](https://github.com/datastax/spark-cassandra-connector). 
`CassandraConnector` takes care of  opening and closing connection to a database, so I simply print debug messages in `open` and `close` 
methods in my `CassandraSinkForeach` class. 

The above code is invoked in the main applications as follows:

```scala
val sink = parsed
	    .writeStream
	    .queryName("KafkaToCassandraForeach")
	    .outputMode("update")
	    .foreach(new CassandraSinkForeach())
	    .start()
```

`CassandraSinkForeach` will be created for each row and each worker will insert its portion of rows into a database. So, each worker 
runs `val cassandraDriver = new CassandraDriver();` This is how `CassandraDriver` class looks like:

```scala
class CassandraDriver extends SparkSessionBuilder {
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
```

Let’s have a closer look at `spark` object in the above code. Here is the code for `SparkSessionBuilder`:

```scala
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
```

On each worker, `SparkSessionBuilder` provides access to `SparkSession` which was created on the driver. To make it work, we 
need to make `SparkSessionBuilder` serializable and use `@transient lazy val` which allows the serialization system to ignore 
the `conf` and `spark` objects. Now, `buildSparkSession` is being serialized and sent to each worker but the `conf` and `spark` objects 
is being resolved when it is needed in the worker.

Now let’s look at the application main body:

```scala
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
    
    // Read incoming stream
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

    // Process data. Create a new date column
    val parsed = jsons
      .withColumn("timestamp_dt", to_date(from_unixtime($"timestamp_ms"/1000.0, "yyyy-MM-dd HH:mm:ss.SSS")))
      .filter("fx_marker != ''")

    // Output results into a database
    val sink = parsed
    .writeStream
    .queryName("KafkaToCassandraForeach")
    .outputMode("update")
    .foreach(new CassandraSinkForeach())
    .start()

    sink.awaitTermination()
  }
}
```

When the application is sent to execution, `buildSparkSession` is being serialized and sent to workers, however `conf` and `spark` 
objects have been left unresolved yet. Next, the driver creates `spark` object inside `KafkaToCassandra` and distributes the work 
among executors. The workers read data from Kafka, make simple transformations, and when workers are ready to write the results 
to the database, they resolve `conf` and `spark` objects thus getting access to `SparkSession` created on the driver.

## How to build and run the app?

When I moved from PySpark to Scala, it took me a while to understand how build the app. So, I included Maven `pom.xml` to the repo. 
You can built the app with Maven by running `mvn package` command. After that you can execute the application using 
`./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1,datastax:spark-cassandra-connector:2.3.0-s_2.11 --class com.insight.app.CassandraSink.KafkaToCassandra --master spark://ec2-18-232-26-53.compute-1.amazonaws.com:7077 target/cassandra-sink-0.0.1-SNAPSHOT.jar`.
