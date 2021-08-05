package org.example

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import org.apache.spark.streaming.kinesis.{KinesisInitialPositions, KinesisInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.dstream.DStream

import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer;

/**
 * Example was taken from <a href="https://github.com/awslabs/real-time-analytics-spark-streaming/blob/master/source/kinesis-java-consumer">Java Kinesis Producer/Consumer</a>
 */
object Main {

  val CSV_FILE_PATH = "data.csv"
  val JSON_FILE_PATH = "data.json"


  val sdf = new SimpleDateFormat("yyyy/MM/dd/HH/mm")

  val KINESIS_APP_NAME_INDEX = 0
  val KINESIS_STREAM_NAME_INDEX = 1
  val REGION_NAME_INDEX = 2
  val S3_OUTPUT_LOCATION_INDEX = 3
  val BATCH_INTERVAL: Duration = Seconds(10)

  val ENDPOINT_URL_PREFIX = "https://kinesis."
  val ENDPOINT_URL_SUFFIX = ".amazonaws.com"

  val S3_SCHEMA_PREFIX = "s3a://"

  def createEndpointUrl(regionName: String): String =
    ENDPOINT_URL_PREFIX + regionName + ENDPOINT_URL_SUFFIX

  def getKinesisNumberOfShards(kinesis: AmazonKinesis, streamName: String): Int =
    kinesis.describeStream(streamName).getStreamDescription.getShards.size()

  def createKinesisStreamList(ssc: StreamingContext, numShards: Int, regionName: String,
                              endpointURL: String, streamName: String, kinesisAppName: String): List[DStream[Array[Byte]]] = {
    val streamList: ListBuffer[DStream[Array[Byte]]] = ListBuffer()
    for (_ <- 0 until numShards) {
      streamList += KinesisInputDStream.builder
        .streamingContext(ssc)
        .regionName(regionName)
        .endpointUrl(endpointURL)
        .streamName(streamName)
        .initialPosition(new KinesisInitialPositions.Latest())
        .checkpointAppName(kinesisAppName)
        .checkpointInterval(BATCH_INTERVAL)
        .build()
    }
    streamList.toList
  }

  def main(args: Array[String]): Unit = {

    //    if (args.length != 1) {
    //      System.err.println("Usage: KinesisConsumer <app-name> <stream-name> <region-name> <output-location>\n\n" + "    <app-name> is the name of the app, used to track the read data in DynamoDB\n" + "    <stream-name> is the name of the Kinesis stream\n" + "    <region-name> region where the Kinesis stream is created\n" + "    <output-location> bucket on S3 where the data should be stored.\n")
    //      System.exit(1)
    //    }
    val outputLocation = "user-bucket-0001/result-data"
    val inputLocation = "user-bucket-0001/input-data"
    val regionName = "us-west-2"
    val endpointURL: String = createEndpointUrl(regionName)
    val streamName = "Foo"
    val kinesisAppName = "application-1"


    val awsCredentials: DefaultAWSCredentialsProviderChain = DefaultAWSCredentialsProviderChain.getInstance()
    val clientBuilder: AmazonKinesisClientBuilder = AmazonKinesisClientBuilder.standard()
      .withEndpointConfiguration(new EndpointConfiguration(endpointURL, regionName))
      .withCredentials(awsCredentials);

    val kinesis: AmazonKinesis = clientBuilder.build()
    val numShards: Int = getKinesisNumberOfShards(kinesis, streamName)
    println(s"Number of shards: $numShards")

    val spark: SparkSession =
      SparkSessionSingleton.getInstance(
        SparkSession.builder().appName(kinesisAppName)/*.master("local[*]")*/, awsCredentials)

//    val ssc: StreamingContext = new StreamingContext(spark.sparkContext, BATCH_INTERVAL)
//    val streamList = createKinesisStreamList(ssc, numShards, regionName, endpointURL, streamName, kinesisAppName)

    import spark.implicits._

    val stream = spark
      .readStream
      .format("kinesis")
      .option("streamName", streamName)
      .option("region", regionName)
      .option("endpointUrl", endpointURL)
      .option("awsAccessKeyId", awsCredentials.getCredentials.getAWSAccessKeyId)
      .option("awsSecretKey", awsCredentials.getCredentials.getAWSSecretKey)
      .option("kinesis.client.avoidEmptyBatches", "true")
      .load()

    stream.printSchema

    val writeStream = stream
      .map(row => {
        val data = row.getAs[Array[Byte]]("data")
        val res = new String(data)
        println(res)
        res
      })
      .writeStream
      .format("text")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .option("checkpointLocation", s"s3a://$outputLocation/checkpoint")
      .start(s"s3a://$outputLocation/")
      .awaitTermination()


  }
}


