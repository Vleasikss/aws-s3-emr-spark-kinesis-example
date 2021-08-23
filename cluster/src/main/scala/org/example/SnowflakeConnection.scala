package org.example

import java.sql.{Connection, DriverManager}
import java.util.Properties

object SnowflakeConnection {

  private val properties = new Properties()
  properties.load(getClass.getResourceAsStream("/application.properties"))

  private val user = properties.getProperty("snowflake.account.username")
  private val password = properties.getProperty("snowflake.account.password")
  private val accountId = properties.getProperty("snowflake.account.id")
  private val db = properties.getProperty("snowflake.database.name")
  private val schema = properties.getProperty("snowflake.schema.name")
  private val warehouse = properties.getProperty("snowflake.warehouse.name")
  private val role = properties.getProperty("snowflake.role")

  private def url: String = s"$accountId.snowflakecomputing.com"

  private def jdbcUrl: String = s"jdbc:snowflake://$url/?db=$db&warehouse=$warehouse&schema=$schema&role=$role"

  /**
   * @return Spark Snowflake connection configuration
   * @see <a href="https://docs.snowflake.com/en/user-guide/spark-connector-use.html#label-spark-options">Spark Snowflake connector</a>
   */
  def getSparkConfiguration: Map[String, String] = Map[String, String](
    "sfUrl" -> url,
    "sfUser" -> user,
    "sfPassword" -> password,
    "sfDatabase" -> db,
    "sfSchema" -> schema,
    "sfWarehouse" -> warehouse,
    "sfRole" -> role
  )

  def getJDBCConnection: Connection = DriverManager.getConnection(jdbcUrl, user, password)


}
