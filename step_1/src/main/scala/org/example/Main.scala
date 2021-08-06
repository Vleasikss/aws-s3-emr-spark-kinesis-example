package org.example

import org.apache.spark.sql.SparkSession

/**
 * Example was taken from <a href="https://github.com/awslabs/real-time-analytics-spark-streaming/blob/master/source/kinesis-java-consumer">Java Kinesis Producer/Consumer</a>
 */
object Main {

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      throw new Error("args must be specified")
    }
    val Array(accessKey, secretKey, inputPath, outputPath) = args
    System.setProperty("aws.accessKeyId", accessKey)
    System.setProperty("aws.secretKey", secretKey)
    val spark = SparkSessionConfigurator.createConfiguredSessionInstance(SparkSession.builder().appName("step-1"))

    val data = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(inputPath)

    data.printSchema()
    println(s"Total number of records: ${data.count()}")
    data.write.format("csv")
      .mode("overwrite")
      .save(outputPath)

  }
}
