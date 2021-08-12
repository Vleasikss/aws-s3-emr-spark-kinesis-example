package org.example

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.sql.SparkSession

/**
 * Just a template of AWS EMR step
 */
object Main {

  val APPLICATION_NAME = "step-1"

  val CSV_FORMAT = "csv"
  val INPUT_DATA_PATH = "s3a://user-bucket-0001/input-data/data.csv"
  val OUTPUT_DATA_PATH = s"s3a://user-bucket-0001/result-data/${System.currentTimeMillis()}"

  /**
   * spark-submit step1.jar aws_access_key aws_secret_key inputPath outputPath
   *
   * @param args aws_access_key aws_secret_key inputPath outputPath
   */
  def main(args: Array[String]): Unit = {
    val awsCredentials: ProfileCredentialsProvider = AwsCredentialsSingleton.getAwsCredentialsProvider
    val spark = SparkSessionConfigurator
      .createConfiguredSessionInstance(SparkSession.builder().appName(APPLICATION_NAME), awsCredentials)

    val data = spark.read
      .text(INPUT_DATA_PATH)

    data.printSchema()
    data.write
      .format(CSV_FORMAT)
      .save(OUTPUT_DATA_PATH)

  }
}
