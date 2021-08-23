**Project Template for Scala Spark Amazon Web Services(S3,EMR,Kinesis)** </br>

sbt clean compile assembly

If you want to use SnowSQL, make sure that your VPN is enabled.

1. Before starting the application as a Spark EMR cluster **make sure that**:
   - you have enough permissions to send the data into S3 (configure it pre-start using EC2 instance profile checkbox); 
   - you have enough permission to declare a cluster (configure it pre-start using EMR role checkbox);
   - endpointUrl in kinesisDInputStream set correctly;
2. Before starting the Spark application locally **make sure that**:
   - your access key && secret keys configured correctly;
   - you configured master param in SparkSession configuration;
