package org.example

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kinesis.{KinesisInitialPositions, KinesisInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import java.text.SimpleDateFormat

/**
 * Example was taken from <a href="https://github.com/awslabs/real-time-analytics-spark-streaming/blob/master/source/kinesis-java-consumer">Java Kinesis Producer/Consumer</a>
 */
object Main {

  val CSV_FILE_PATH = "data.csv"
  val JSON_FILE_PATH = "data.json"

  val sdf = new SimpleDateFormat("yyyy/MM/dd/HH/mm")

  val BATCH_INTERVAL: Duration = Seconds(10)

  val ENDPOINT_URL_PREFIX = "https://kinesis."
  val ENDPOINT_URL_SUFFIX = ".amazonaws.com"

  def createEndpointUrl(regionName: String): String = ENDPOINT_URL_PREFIX + regionName + ENDPOINT_URL_SUFFIX

  def getKinesisNumberOfShards(kinesis: AmazonKinesis, streamName: String): Int =
    kinesis.describeStream(streamName).getStreamDescription.getShards.size()

  def createKinesisStreamList(ssc: StreamingContext, numShards: Int, regionName: String,
                              endpointURL: String, streamName: String, kinesisAppName: String): List[DStream[Array[Byte]]] =
    (0 until numShards).map(_ =>
      KinesisInputDStream.builder
        .streamingContext(ssc)
        .regionName(regionName)
        .endpointUrl(endpointURL)
        .streamName(streamName)
        .initialPosition(new KinesisInitialPositions.Latest())
        .checkpointAppName(kinesisAppName)
        .checkpointInterval(BATCH_INTERVAL)
        .build()
    ).toList

  /**
   * Checkpointing is actually a feature of Spark Core (that Spark SQL uses for distributed computations)
   * that allows a driver to be restarted
   * on failure with previously computed state of a distributed computation described as an RDD.
   *
   * That has been successfully used in
   * Spark Streaming - the now-obsolete Spark module for stream processing based on RDD API.
   *
   * @see <a href="https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-checkpointing.html">Dataset checkpointing </a>
   */
  val CHECKPOINT_LOCATION_KEY = "checkpointLocation"
  val CHECKPOINT_LOCATION_VALUE_FUNC: String => String = (outputS3Location: String) => s"s3a://$outputS3Location/checkpoint"


  /**
   * spark-submit root.jar app-name stream-name region-name s3-directory-output-location profile-name
   *
   * @param args - app-name, stream-name, region-name, s3-directory-output-location, profile-name(by default is 'default')
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: KinesisConsumer <app-name> <stream-name> <region-name> <s3-directory-output-location>\n\n" +
        "    <app-name> is the name of the app, used to track the read data in DynamoDB\n" +
        "    <stream-name> is the name of the Kinesis stream\n" +
        "    <region-name> region where the Kinesis stream is created\n" +
        "    <s3-directory-output-location> bucket on S3 where the data should be stored.\n" +
        "    <profile-name> OPTIONAL. aws profile name.\n")
      System.exit(1)
    }
    val Array(kinesisAppName, streamName, regionName, outputLocation) = args
    val profileName = if (args.length >= 5) args(4) else null
    val endpointURL: String = createEndpointUrl(regionName)

    val awsCredentials = AwsCredentialsSingleton.getAwsCredentialsProvider(profileName)
    val clientBuilder = AmazonKinesisClientBuilder.standard()
      .withEndpointConfiguration(new EndpointConfiguration(endpointURL, regionName))
      .withCredentials(awsCredentials)

    val kinesis: AmazonKinesis = clientBuilder.build()
    val numShards: Int = getKinesisNumberOfShards(kinesis, streamName)
    println(s"Number of shards: $numShards")

    val spark: SparkSession = SparkSessionConfigurator
      .createConfiguredSessionInstance(SparkSession.builder().appName(kinesisAppName), awsCredentials)

    val ssc: StreamingContext = new StreamingContext(spark.sparkContext, BATCH_INTERVAL)
    val streamList = createKinesisStreamList(ssc, numShards, regionName, endpointURL, streamName, kinesisAppName)

    val unionStreams: DStream[Array[Byte]] = ssc.union(streamList)
    unionStreams
      .map(new String(_))
      .filter(_.nonEmpty)
      .foreachRDD(_.coalesce(1).saveAsTextFile(s"s3a://$outputLocation/${System.currentTimeMillis()}"))

    ssc.start()
    ssc.awaitTermination()

  }
}
