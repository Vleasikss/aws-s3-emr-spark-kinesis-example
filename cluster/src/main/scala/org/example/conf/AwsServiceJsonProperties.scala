package org.example.conf

import java.io.{File, FileNotFoundException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

trait AwsServiceJsonProperties {

  protected val serviceName: String

  protected val POLICY_ATTRIBUTE = "Policy"

  /**
   * Each policy file in a service has to be named so
   */
  private val POLICY_FILE = "policy.json"

  /**
   * Directory location where services' configurations must be located
   */
  private val SERVICE_FOLDER = "service/"

  /**
   * @return file configuration array that contains: [policy.json, permission.json, etc]
   */
  val files: Array[File] = {
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
