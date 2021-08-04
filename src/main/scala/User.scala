package org.example

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}

import java.nio.charset.StandardCharsets

case class User(firstName: String, lastName: String, id: String, ip: String) {

  private val JSON: ObjectMapper = new ObjectMapper()
  JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  override def toString: String = "org.example.User{" + "firstName='" + firstName + '\'' + ", lastName='" + lastName + '\'' + ", id='" + id + '\'' + ", ip='" + ip + '\'' + '}'
}

object User {

  /**
   *
   * @param bytes - byte array of string formatted in: firstName1, lastName1, id1, ip1
   * @return new User parsed from byte array
   */
  def fromTextAsBytes(bytes: Array[Byte]): User = {
    val value = new String(bytes, StandardCharsets.UTF_8)
    val parts = value.split(",")
    if (parts.length != 4) return null
    new User(parts(0), parts(1), parts(2), parts(3))
  }
}

