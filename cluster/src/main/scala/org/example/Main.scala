package org.example

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.streaming.{Duration, Seconds}

/**
 * Example was taken from <a href="https://github.com/awslabs/real-time-analytics-spark-streaming/blob/master/source/kinesis-java-consumer">Java Kinesis Producer/Consumer</a>
 */
object Main extends Logging {

  val BATCH_DURATION: Duration = Seconds(10)

  val ENDPOINT_URL_PREFIX = "https://kinesis."
  val ENDPOINT_URL_SUFFIX = ".amazonaws.com"

  //language=SQL
  private val FIND_ALL_USERS_QUERY = "SELECT * FROM USERS"

  /**
   * Spark uses log4j for logging.
   * You can configure it by adding a log4j.properties file in the conf directory.
   *
   * @see <a href="https://spark.apache.org/docs/2.4.2/configuration.html#configuring-logging">Spark documentation. Log4j configuring</a>
   */
  def configureLogging(): Unit =
    PropertyConfigurator.configure(getClass.getResourceAsStream("/conf/log4j.properties"))

  /**
   * spark-submit root.jar app-name stream-name region-name s3-directory-output-location profile-name
   *
   * @param args - app-name, stream-name, region-name, s3-directory-output-location, profile-name(by default is 'default')
   */
  def main(args: Array[String]): Unit = {
    configureLogging()

    val connection = SnowflakeConnector.getConnection
    val statement = connection.createStatement
    val resultSet = statement.executeQuery(FIND_ALL_USERS_QUERY)
    val metaData = resultSet.getMetaData

    logger.info("Number of Columns : " + metaData.getColumnCount)
    while (resultSet.next()) {
      println(resultSet.getString("FIRSTNAME"))
      println(resultSet.getString("LASTNAME"))
      println(resultSet.getString("AGE"))
    }

    resultSet.close()
    statement.close()
    connection.close()
  }
}
