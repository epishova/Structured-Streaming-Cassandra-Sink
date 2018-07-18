# Structured-Streaming-Cassandra-Sink
### An example of how to create and use Cassandra sink in Spark Structured Streaming application

This code was developed as part of the Insight Data Engineering program. This is a simple example of how to create and use Cassandra sink in Spark Structured Streaming. I hope it will be useful for those who have just begun to work with Structured Streaming API. I am new to it too, so comments and suggestions on how to improve the application are very welcome.

The idea of this application is very simple. It reads messages from Kafka, parses them, and saves them into Cassandra. This example was run on AWS cluster, so if you'd like to test it just replace the addresses of my AWS instances with yours (everything that looks like `ec2-xx-xxx-xx-xx.compute-1.amazonaws.com`).

This repo contains `pom.xml` and can be built with Maven by `mvn package`. After that you can execute the application using
`./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1,datastax:spark-cassandra-connector:2.3.0-s_2.11 --class com.insight.app.CassandraSink.KafkaToCassandra --master spark://ec2-18-232-26-53.compute-1.amazonaws.com:7077 target/cassandra-sink-0.0.1-SNAPSHOT.jar`.

Additionally, [this link] (https://stackoverflow.com/questions/34457486/why-does-spark-fail-with-failed-to-get-broadcast-0-piece0-of-broadcast-0-in-lo) helped me to understand the usage of `lazy val` in Spark. 
