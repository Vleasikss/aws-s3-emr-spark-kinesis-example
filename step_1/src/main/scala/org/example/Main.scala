package org.example

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.sql.SparkSession

object Main {

  /**
   * spark-submit step1.jar aws_access_key aws_secret_key inputPath outputPath
   *
   * @param args aws_access_key aws_secret_key inputPath outputPath
   */
  def main(args: Array[String]): Unit = {
//    if (args.length != 2) {
//      throw new Error("args must be specified")
//    }
//    val Array(inputPath, outputPath) = args
    val awsCredentials: ProfileCredentialsProvider = AwsCredentialsSingleton.getAwsCredentialsProvider
    val spark = SparkSessionConfigurator
      .createConfiguredSessionInstance(SparkSession.builder().appName("step-1"), awsCredentials)

    val data = spark.read
      .text(s"s3a://user-bucket-0001/input-data/data.csv")

    data.printSchema()
    data.write
      .format("csv")
      .save(s"s3a://user-bucket-0001/result-data/${System.currentTimeMillis()}")

  }
}
