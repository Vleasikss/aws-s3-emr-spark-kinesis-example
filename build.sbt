name := "apache-spark-emr-s3-kinesis"
version := "0.1"
scalaVersion := "2.12.8"
idePackagePrefix := Some("org.example")

val SPARK_VERSION = "3.1.1"
val TYPESAFE_VERSION = "1.4.1"
val SCALAJ_HTTP_VERSION = "2.3.0"
val AWS_JAVA_SDK_VERSION = "1.12.27"
val HADOOP_VERSION = "3.2.1"
val SNOWFLAKE_JDBC_VERSION = "3.13.6"
val SPARK_SNOWFLAKE_VERSION = "2.9.1-spark_3.1"
lazy val root = (project in file("cluster"))
  .settings(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.9",
      "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided",
      "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
      "org.apache.spark" %% "spark-hive" % SPARK_VERSION,
      "org.apache.spark" %% "spark-mllib" % SPARK_VERSION,
      "org.apache.spark" %% "spark-streaming" % SPARK_VERSION,
      "com.amazonaws" % "aws-java-sdk" % AWS_JAVA_SDK_VERSION,
      "com.typesafe" % "config" % TYPESAFE_VERSION,
      "org.apache.spark" %% "spark-streaming-kinesis-asl" % SPARK_VERSION,
      "org.scalaj" %% "scalaj-http" % SCALAJ_HTTP_VERSION,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.1",
      "org.apache.hadoop" % "hadoop-common" % HADOOP_VERSION,
      "org.apache.hadoop" % "hadoop-client" % HADOOP_VERSION,
      "org.apache.hadoop" % "hadoop-aws" % HADOOP_VERSION,
      "net.snowflake" % "snowflake-jdbc" % SNOWFLAKE_JDBC_VERSION,
      "net.snowflake" %% "spark-snowflake" % SPARK_SNOWFLAKE_VERSION
    ),
    assemblySettings
  )
lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
)
