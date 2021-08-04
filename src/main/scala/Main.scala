package org.example

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.{KinesisInitialPositions, KinesisInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import org.apache.spark.SparkConf
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
  val BATCH_INTERVAL: Duration = Seconds(120)

  val ENDPOINT_URL_PREFIX = "https://kinesis."
  val ENDPOINT_URL_SUFFIX = ".amazonaws.com"

  def createEndpointUrl(regionName: String): String = ENDPOINT_URL_PREFIX + regionName + ENDPOINT_URL_SUFFIX

  def getNumberOfShards(kinesis: AmazonKinesis, streamName: String): Int = {
    kinesis.describeStream(streamName).getStreamDescription.getShards.size()
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      System.err.println("Usage: KinesisConsumer <app-name> <stream-name> <region-name> <output-location>\n\n" + "    <app-name> is the name of the app, used to track the read data in DynamoDB\n" + "    <stream-name> is the name of the Kinesis stream\n" + "    <region-name> region where the Kinesis stream is created\n" + "    <output-location> bucket on S3 where the data should be stored.\n")
      System.exit(1)
    }

    val Array(kinesisAppName, streamName, regionName, outputLocation) = args
    val endpointURL: String = createEndpointUrl(regionName)

    val awsCredentials: DefaultAWSCredentialsProviderChain = DefaultAWSCredentialsProviderChain.getInstance()
    val clientBuilder: AmazonKinesisClientBuilder = AmazonKinesisClientBuilder.standard()
      .withEndpointConfiguration(new EndpointConfiguration(endpointURL, regionName))
      .withCredentials(awsCredentials);

    val kinesis: AmazonKinesis = clientBuilder.build()
    val numShards: Int = getNumberOfShards(kinesis, streamName)
    println(s"Number of shards: $numShards")

    val spark: SparkConf = new SparkConf()
      //      .setMaster("local[*]")
      .setAppName(kinesisAppName)

    val ssc = new StreamingContext(spark, BATCH_INTERVAL)

    val streamList: ListBuffer[DStream[Array[Byte]]] = ListBuffer()
    for (i <- 0 until numShards) {
      streamList += KinesisInputDStream.builder
        .streamingContext(ssc)
        .regionName(regionName)
        .endpointUrl(endpointURL)
        .streamName(streamName)
        .initialPosition(new KinesisInitialPositions.Latest())
        .checkpointAppName(kinesisAppName)
        .checkpointInterval(BATCH_INTERVAL)
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
    }

    val list: List[DStream[Array[Byte]]] = streamList.toList
    val unionStreams: DStream[Array[Byte]] = ssc.union(list)
    unionStreams
      .map(byteArray => if (!byteArray.isEmpty) byteArray.map(_.toChar).mkString)
      .repartition(1)
      .saveAsTextFiles("s3a://" + outputLocation)


    ssc.start()
    ssc.awaitTermination()

  }
}


