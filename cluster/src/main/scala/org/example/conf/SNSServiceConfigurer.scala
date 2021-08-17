package org.example.conf

import com.amazonaws.services.sns.model.SetTopicAttributesRequest
import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClientBuilder}

class SNSServiceConfigurer(topicArn: String, sns: AmazonSNS = AmazonSNSClientBuilder.defaultClient()) extends AwsServiceJsonProperties {
  override protected val serviceName: String = "sns"


  def configurePolicies(): Unit = {
    val policy = getPolicyFile
    sns.setTopicAttributes(new SetTopicAttributesRequest(topicArn, POLICY_ATTRIBUTE, fileToString(policy)))
  }

}