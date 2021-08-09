package org.example

import com.amazonaws.auth.profile.ProfileCredentialsProvider

object AwsCredentialsSingleton {

  private val DEFAULT_PROFILE_NAME = "default"

  /**
   *
   * AWS credentials provider chain that looks for credentials in this order:
   *  - Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (RECOMMENDED since they are recognized by all the AWS SDKs and CLI except for .NET), or AWS_ACCESS_KEY and AWS_SECRET_KEY (only recognized by Java SDK)
   *    export AWS_ACCESS_KEY_ID=""
   *    export AWS_SECRET_ACCESS_KEY=""
   *  - Java System Properties - aws.accessKeyId and aws.secretKey
   *  - Web Identity Token credentials from the environment or container
   *  - <strong>IN USING</strong>.Credential profiles file at the default location (~/.aws/credentials) shared by all AWS SDKs and the AWS CLI
   *  - Credentials delivered through the Amazon EC2 container service if AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" environment variable is set and security manager has permission to access the variable,
   *  - Instance profile credentials delivered through the Amazon EC2 metadata service
   *
   * @see <a href="https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html">Working with AWS Credentials</a>
   */
  def getAwsCredentials(profileName: String): ProfileCredentialsProvider = {
    new ProfileCredentialsProvider(validateProfileName(profileName))
  }

  def validateProfileName(profileName: String): String =
    if (profileName == null || profileName.isEmpty) DEFAULT_PROFILE_NAME else profileName

}
