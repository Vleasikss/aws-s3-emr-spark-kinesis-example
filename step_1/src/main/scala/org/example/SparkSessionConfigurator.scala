package org.example

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}

object SparkSessionConfigurator {

  val conf: Config = ConfigFactory.load("application.conf")

  private object Spark {

    /**
     * @see <a href="https://spark.apache.org/docs/latest/tuning.html"> Spark docs </a>
     *
     *      There are two types of serialization:
     *        - java.io.Serializable
     *        - org.apache.spark.serializer.KryoSerializer
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/io/Serializable.html">java.io.Serializable</a>.
     *       - Uses by default
     *       - Can work with any class you create that implements java.io.Serializable;
     *       - Can also be performance controlled by extending java.io.Externalizable;
     *       - Serializes quite slow;
     * @see <a href="https://github.com/EsotericSoftware/kryo>org.apache.spark.serializer.KryoSerializer</a>
     *      - Serializes objects more quickly;
     *      - Does not support all Serializable types and requires
     *        to register the classes you'll use in the program in advance for best performance;
     *
     */
    val SERIALIZER_KEY = "spark.serializer"

    /**
     * @see <a href="https://spark.apache.org/docs/latest/tuning.html">Apache Spark docs</a> </br>
     *
     *      org.apache.spark.serializer.KryoSerializer
     *
     *      Recommended to be used in any network-intensive application.
     * @since Spark v2.0.0, Spark internally uses Kryo serializer
     *        when shuffling RDDs with simple types,
     *        arrays of simple types, or string type.
     */
    val SERIALIZER_VALUE: String = conf.getString(SERIALIZER_KEY)

    /**
     * @see <a href="https://yousry.medium.com/spark-speculative-execution-in-10-lines-of-code-3c6e4815875b">Apache Spark Speculative tasks article</a>
     *
     *      At the level of a single stage in a Spark job,
     *      Spark monitors the time needed to complete tasks in the stage.
     *      If some task(s) takes much more time (more on that later) than other ones in same stage,
     *      Spark will resubmit a new copy of same task on another worker node.
     *      Now we have 2 identical tasks running in parallel and when one of them completes successfully,
     *      Spark will kill the other one and pick the output of the successful task and move on.
     */
    val ENABLE_SPECULATION_KEY = "spark.speculation"

    /**
     * @since spark 2.4.4 by default is false
     *
     *        Enabled to false to improve the performance
     */
    val ENABLE_SPECULATION_VALUE: String = conf.getString(ENABLE_SPECULATION_KEY)

    /**
     * Uses to work with EMR
     *
     * @see <a href="https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-committer-reqs.html">Requirements for the EMRFS S3-optimized committer</a>
     */
    object Sql {

      val HIVE_CONVERT_METASTORE_PARQUET_KEY = "spark.sql.hive.convertMetastoreParquet"
      val HIVE_CONVERT_METASTORE_PARQUET_VALUE: String = conf.getString(HIVE_CONVERT_METASTORE_PARQUET_KEY)

      val SOURCES_COMMIT_PROTOCOL_CLASS_KEY = "org.apache.spark.sql.execution.datasources.SQLEmrOptimizedCommitProtocol"
      val SOURCES_COMMIT_PROTOCOL_CLASS_VALUE = "true"

      object Parquet {
        val OUTPUT_COMMITTER_CLASS_KEY = "spark.sql.parquet.output.committer.class"
        val OUTPUT_COMMITTER_CLASS_VALUE: String = conf.getString(OUTPUT_COMMITTER_CLASS_KEY)

        val FS_OPTIMIZED_COMMITTER_OPTIMIZATION_ENABLED_KEY = "spark.sql.parquet.fs.optimized.committer.optimization-enabled"
        val FS_OPTIMIZED_COMMITTER_OPTIMIZATION_ENABLED_VALUE: String = conf.getString(FS_OPTIMIZED_COMMITTER_OPTIMIZATION_ENABLED_KEY)

      }


    }

    object Hadoop {

      /**
       * @see <a href="http://www.openkb.info/2019/04/what-is-difference-between.html"> Difference between fileoutputComitter v1 and v2 </a>
       * @see <a href="https://hadoop.apache.org/docs/r2.7.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml">Hadoop default config docs </a>
       * @version 1.
       *          will do mergePaths() in the end after all reducers complete
       *          If this MR job has many reduces, AM will firstly
       *          wait for all reducers to finish and
       *          then use a single thread to merge the output files.
       * @version 2 - recommended.
       *          will do mergePaths() to move their output files into the final output directory concurrently.
       *          Upgrades performance.
       *
       *
       */
      val MAP_REDUCE_FILE_OUTPUT_COMMITTER_ALGORITHM_VERSION_KEY =
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version"

      /**
       * Enabled to true to improve the performance
       */
      val MAP_REDUCE_FILE_OUTPUT_COMMITTER_ALGORITHM_VERSION_VALUE: String =
        conf.getString(MAP_REDUCE_FILE_OUTPUT_COMMITTER_ALGORITHM_VERSION_KEY)

      object S3 {

        /**
         * If is true, multiple single-object delete requests are
         * replaced by a single 'delete multiple objects'-request,
         * reducing the number of requests.
         *
         * Beware: legacy S3-compatible object stores might not support this request.
         *
         * @see <a href="https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html"> Hadoop config docs </a>
         *
         */
        val MULTI_OBJECT_DELETE_ENABLE_KEY = "spark.hadoop.fs.s3a.multiobjectdelete.enable"

        /**
         * Enabled to false to avoid MultiObjectDeleteException
         */
        val MULTI_OBJECT_DELETE_ENABLE_VALUE: String = conf.getString(MULTI_OBJECT_DELETE_ENABLE_KEY)

        @deprecated
        val FAST_UPLOAD_KEY = "spark.hadoop.fs.s3a.fast.upload"
        @deprecated
        val FAST_UPLOAD_VALUE: String = conf.getString(FAST_UPLOAD_KEY)

        // s3 access key
        val ACCESS_KEY_KEY = "spark.hadoop.fs.s3a.access.key"
        // s3 secret key
        val SECRET_KEY_KEY = "spark.hadoop.fs.s3a.secret.key"

      }

    }
  }

  /**
   * AWS credentials provider chain that looks for credentials in this order:
   *  - <strong>IN USING</strong>. Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (RECOMMENDED since they are recognized by all the AWS SDKs and CLI except for .NET), or AWS_ACCESS_KEY and AWS_SECRET_KEY (only recognized by Java SDK)
   *    export AWS_ACCESS_KEY_ID=""
   *    export AWS_SECRET_ACCESS_KEY=""
   *  - Java System Properties - aws.accessKeyId and aws.secretKey
   *  - Web Identity Token credentials from the environment or container
   *  - Credential profiles file at the default location (~/.aws/credentials) shared by all AWS SDKs and the AWS CLI
   *  - Credentials delivered through the Amazon EC2 container service if AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" environment variable is set and security manager has permission to access the variable,
   *  - Instance profile credentials delivered through the Amazon EC2 metadata servic
   */
  def getAwsCredentials: AWSCredentials =
    DefaultAWSCredentialsProviderChain.getInstance().getCredentials

  def createConfiguredSessionInstance(sparkSession: SparkSession.Builder, awsCredentials: AWSCredentialsProvider): SparkSession = {
    val credentials = awsCredentials.getCredentials
    val spark: SparkSession = sparkSession
      //      .config(Spark.SERIALIZER_KEY, Spark.SERIALIZER_VALUE)
      //      .config("spark.sql.parquet.filterPushdown", "true")
      //      .config("spark.sql.parquet.mergeSchema", "false")
      //      .config(Spark.ENABLE_SPECULATION_KEY, Spark.ENABLE_SPECULATION_VALUE)
      //      .config(Spark.Hadoop.MAP_REDUCE_FILE_OUTPUT_COMMITTER_ALGORITHM_VERSION_KEY, Spark.Hadoop.MAP_REDUCE_FILE_OUTPUT_COMMITTER_ALGORITHM_VERSION_VALUE)
      //      .config(Spark.Hadoop.S3.MULTI_OBJECT_DELETE_ENABLE_KEY, Spark.Hadoop.S3.MULTI_OBJECT_DELETE_ENABLE_VALUE)
      //      .config(Spark.Hadoop.S3.FAST_UPLOAD_KEY, Spark.Hadoop.S3.FAST_UPLOAD_VALUE)
      .config(Spark.Sql.Parquet.FS_OPTIMIZED_COMMITTER_OPTIMIZATION_ENABLED_KEY, Spark.Sql.Parquet.FS_OPTIMIZED_COMMITTER_OPTIMIZATION_ENABLED_VALUE)
      .config(Spark.Sql.HIVE_CONVERT_METASTORE_PARQUET_KEY, Spark.Sql.HIVE_CONVERT_METASTORE_PARQUET_VALUE)
      .config(Spark.Sql.Parquet.OUTPUT_COMMITTER_CLASS_KEY, Spark.Sql.Parquet.OUTPUT_COMMITTER_CLASS_VALUE)
      .config(Spark.Sql.SOURCES_COMMIT_PROTOCOL_CLASS_KEY, Spark.Sql.SOURCES_COMMIT_PROTOCOL_CLASS_VALUE)
      .config(Spark.Hadoop.S3.ACCESS_KEY_KEY, credentials.getAWSAccessKeyId)
      .config(Spark.Hadoop.S3.SECRET_KEY_KEY, credentials.getAWSSecretKey)
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
    spark
  }

  def createConfiguredSessionInstance(sparkSession: SparkSession.Builder): SparkSession =
    this.createConfiguredSessionInstance(sparkSession, DefaultAWSCredentialsProviderChain.getInstance())

}
