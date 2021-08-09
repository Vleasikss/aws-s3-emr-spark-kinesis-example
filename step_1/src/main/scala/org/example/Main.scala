package org.example

import org.apache.spark.sql.SparkSession

object Main {

  /**
   * spark-submit step1.jar aws_access_key aws_secret_key inputPath outputPath
   *
   * @param args aws_access_key aws_secret_key inputPath outputPath
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      throw new Error("args must be specified")
    }
    val Array(accessKey, secretKey, inputPath, outputPath) = args
    System.setProperty("aws.accessKeyId", accessKey)
    System.setProperty("aws.secretKey", secretKey)
    val spark = SparkSessionConfigurator
      .createConfiguredSessionInstance(SparkSession.builder().appName("step-1"))

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
