package org.example.policy

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

class S3ServiceConfigurer(bucketName: String) extends AwsServiceJsonProperties  {
  override protected val serviceName: String = "s3"

  val s3: AmazonS3 = AmazonS3ClientBuilder.defaultClient()

  override def configurePolicies(): Unit = {
    val policy = getPolicyFile
    //      s3.setBucketNotificationConfiguration(new SetBucketNotificationConfigurationRequest(etc))
  }
}