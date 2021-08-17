package org.example.policy

import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClientBuilder}
import com.amazonaws.services.sns.model.SetTopicAttributesRequest

class SNSServiceConfigurer(topicArn: String) extends AwsServiceJsonProperties {
  override protected val serviceName: String = "sns"

  val sns: AmazonSNS = AmazonSNSClientBuilder.defaultClient()

  def configurePolicies(): Unit = {
    val policy = getPolicyFile
    sns.setTopicAttributes(new SetTopicAttributesRequest(topicArn, POLICY_ATTRIBUTE, fileToString(policy)))
  }

}
