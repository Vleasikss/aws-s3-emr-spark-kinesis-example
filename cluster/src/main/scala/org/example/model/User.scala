package org.example.model

import java.nio.charset.StandardCharsets

case class User(firstName: String, lastName: String, age: Int, ip: String) {

  override def toString: String = "org.example.User{" + "firstName='" + firstName + '\'' + ", lastName='" + lastName + '\'' + ", age='" + age + '\'' + ", ip='" + ip + '\'' + '}'

}

object User extends BasicModel {

  override val tableName: String = "users"

  private val USER_PARAMS_SEPARATOR = ","

  /**
   *
   * @param bytes - byte array of string formatted in: firstName1, lastName1, age1, ip1
   * @return new User parsed from byte array
   */
  def fromTextAsBytes(bytes: Array[Byte]): User = {
    val value = new String(bytes, StandardCharsets.UTF_8)
    val parts = value.split(USER_PARAMS_SEPARATOR)
    if (parts.length != 4) return null
    new User(parts(0), parts(1), parts(2).trim.toInt, parts(3))
  }

}

