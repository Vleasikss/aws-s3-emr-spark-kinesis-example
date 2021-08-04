package org.example

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionSingleton {
  @volatile var spark: SparkSession = _

  def getInstance(sparkConf: SparkConf, awsCredentials: DefaultAWSCredentialsProviderChain): SparkSession = {
    synchronized {
      if (spark == null) {
        val builder = SparkSession.builder()
          .config(sparkConf)

        spark = SparkSessionConfigurator
          .config(builder, awsCredentials)
          .getOrCreate()

      }
      spark
    }
  }
}