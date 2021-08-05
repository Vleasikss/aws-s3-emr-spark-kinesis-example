package org.example

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionSingleton {
  @volatile var spark: SparkSession = _

  def getInstance(sparkBuilder: SparkSession.Builder, awsCredentials: DefaultAWSCredentialsProviderChain): SparkSession = {
    synchronized {
      if (spark == null) {
        spark = SparkSessionConfigurator.config(sparkBuilder, awsCredentials)
      }
      spark
    }
  }
}