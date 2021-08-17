package org.example.s3

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{BucketNotificationConfiguration, Filter, FilterRule, QueueConfiguration, S3Event, S3KeyFilter, SetBucketNotificationConfigurationRequest, TopicConfiguration}
import org.example.Logging
import org.example.Main.logger

import java.util
import java.util.stream.Collectors
import java.util.stream.Collectors.joining
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class S3NotificationBuilder(credentialsProvider: AWSCredentialsProvider, regionName: String, s3BucketName: String) extends Logging {

  import S3NotificationBuilder._

  private val s3Client = AmazonS3ClientBuilder.standard()
    .withCredentials(credentialsProvider)
    .withRegion(regionName)
    .build()

  private val notificationConfiguration = new BucketNotificationConfiguration()


  /**
   * adds notification to SNS on receiving `FILE_ON_SUCCESS_S3_MOVING` in S3 bucket
   *
   * @param snsTopicArn send notification to sns topic by ARN
   * @return this
   */
  def withNotificationOnReceivingTransformedSparkFilesToSNS(snsTopicArn: String): S3NotificationBuilder = {
    val topicConfiguration = new TopicConfiguration(snsTopicArn, util.EnumSet.of(S3Event.ObjectCreatedByPut))
    topicConfiguration.setFilter(getSuffixFilter(FILE_ON_SUCCESS_S3_MOVING))
    // Add an SNS topic notification.
    notificationConfiguration.addConfiguration(SNS_TOPIC_CONFIG_KEY, topicConfiguration)

    this
  }

  /**
   * adds notification to SQS on receiving `FILE_ON_SUCCESS_S3_MOVING` in S3 bucket
   *
   * @param sqsTopicArn send notification to sqs queue by ARN
   * @return this
   */
  def withNotificationOnReceivingTransformedSparkFilesToSQS(sqsTopicArn: String): S3NotificationBuilder = {
    val filter = getSuffixFilter(FILE_ON_SUCCESS_S3_MOVING)
    val queueConfiguration = new QueueConfiguration(sqsTopicArn, util.EnumSet.of(S3Event.ObjectCreatedByPut))
    queueConfiguration.withFilter(filter)
    // Add an SQS topic configuration
    notificationConfiguration.addConfiguration(SQS_TOPIC_CONFIG_KEY, queueConfiguration)

    this
  }

  /**
   * appends configured notifications to S3 bucket
   *
   * @return AmazonS3 client
   */
  def build(): AmazonS3 = {
    val request = new SetBucketNotificationConfigurationRequest(s3BucketName, notificationConfiguration)
    s3Client.setBucketNotificationConfiguration(request)

    s3Client.getBucketNotificationConfiguration(s3BucketName).getConfigurations.forEach((confName, configuration) => {
      val events = configuration.getEvents
        .map({ x: String => x })
        .mkString(", ")

      val filterRules = configuration.getFilter
        .getS3KeyFilter.getFilterRules
        .map(str => s"${str.getName} = ${str.getValue}")
        .mkString(", ")

      logger.info(s"created new notification configuration from s3Bucket $s3BucketName to $confName: {events = [$events]}, {filterRules = [$filterRules]}")
    })
    s3Client
  }


  /**
   * creates a filter. If there is a file that starts with `fileName`, create a notification
   *
   * @param fileName filename
   * @return filter on s3Files.startsWith(fileName)
   */
  private def getSuffixFilter(fileName: String): Filter = {
    val filter = new Filter()
    val filterRules = new S3KeyFilter()
    filterRules.addFilterRule(S3KeyFilter.FilterRuleName.Suffix.newRule(fileName))
    filter.setS3KeyFilter(filterRules)
    filter
  }

}
object S3NotificationBuilder {

  /**
   * Indicator file which tells that all the files were moved successfully
   */
  private val FILE_ON_SUCCESS_S3_MOVING = "_SUCCESS"

  /**
   * configuration key for SNS for `BucketNotificationConfiguration`
   */
  private val SNS_TOPIC_CONFIG_KEY = "snsTopicConfig"

  /**
   * configuration key for SQS for `BucketNotificationConfiguration`
   */
  private val SQS_TOPIC_CONFIG_KEY = "sqsTopicConfig"

}