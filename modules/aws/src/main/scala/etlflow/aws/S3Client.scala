package etlflow.aws

import etlflow.log.ApplicationLogger
import etlflow.model.Credential.AWS
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3AsyncClientBuilder}

import java.net.URI

object S3Client extends ApplicationLogger {
  def apply(region: Region, credentials: Option[AWS] = None, endpointOverride: Option[String] = None): S3AsyncClient = {
    val accessKey = sys.env.getOrElse("ACCESS_KEY", "NOT_SET_IN_ENV")
    val secretKey = sys.env.getOrElse("SECRET_KEY", "NOT_SET_IN_ENV")

    val initBuilder: S3AsyncClientBuilder = credentials match {
      case Some(creds) =>
        logger.info("Using AWS credentials from credentials passed in function")
        val credentials =
          StaticCredentialsProvider.create(AwsBasicCredentials.create(creds.accessKey, creds.secretKey))
        S3AsyncClient.builder.region(region).credentialsProvider(credentials)
      case None =>
        (accessKey, secretKey) match {
          case (access_key, secret_key) if access_key == "NOT_SET_IN_ENV" || secret_key == "NOT_SET_IN_ENV" =>
            logger.info("Using AWS credentials from local sdk")
            S3AsyncClient.builder.region(region)
          case keys =>
            logger.info("Using AWS credentials from environment variables(ACCESS_KEY,SECRET_KEY)")
            val credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(keys._1, keys._2))
            S3AsyncClient.builder.region(region).credentialsProvider(credentials)
        }
    }

    val client = endpointOverride
      .map(URI.create)
      .map(initBuilder.endpointOverride)
      .getOrElse(initBuilder)
      .build

    client
  }

}
