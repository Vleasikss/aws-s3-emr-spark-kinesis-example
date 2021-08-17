package org.example.policy

import com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client, AmazonS3ClientBuilder}
import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClientBuilder}
import com.amazonaws.services.sns.model.SetTopicAttributesRequest

import java.io.{File, FileNotFoundException}
import java.nio.file.{Files, Path, Paths}
import java.nio.charset.StandardCharsets


trait AwsServiceJsonProperties {

  protected val serviceName: String

  protected val POLICY_ATTRIBUTE = "Policy"

  private val POLICY_FILE = "policy.json"

  private val SERVICE_FOLDER = "service/"

  /**
   *
   * @return file array that contains: [policy.json, permission.json, etc]
   */
  def files: Array[File] = {
    val loader = getClass
    val url = loader.getResource(s"/$SERVICE_FOLDER$serviceName")
    val path = url.getPath
    new File(path).listFiles
  }

  def getPolicyFile: File = {
   files.find(_.getName.equals(POLICY_FILE))
      .getOrElse(throw new FileNotFoundException("policy file not exists"))
  }
  def configurePolicies(): Unit

  def fileToString(file: File): String = {
    val result = Files.readString(Paths.get(file.getPath), StandardCharsets.UTF_8)
    result
  }

}
