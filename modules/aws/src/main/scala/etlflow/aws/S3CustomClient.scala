package etlflow.aws

import etlflow.schema.Location.S3
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

import java.net.URI

private[etlflow] object S3CustomClient {
  def apply(Location: S3, endpointOverride: Option[String] = None): S3AsyncClient = {
    val ACCESS_KEY = sys.env.getOrElse("ACCESS_KEY", "NOT_SET_IN_ENV")
    val SECRET_KEY = sys.env.getOrElse("SECRET_KEY", "NOT_SET_IN_ENV")

    val initBuilder = Location.credentials match {
      case Some(creds) =>
        logger.info("Using AWS credentials from credentials passed in function")
        val credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(creds.access_key, creds.secret_key))
        S3AsyncClient.builder.region(Region.of(Location.region)).credentialsProvider(credentials)
      case None => (ACCESS_KEY, SECRET_KEY) match {
        case (access_key, secret_key) if access_key == "NOT_SET_IN_ENV" || secret_key == "NOT_SET_IN_ENV" =>
          logger.info("Using AWS credentials from local sdk")
          S3AsyncClient.builder.region(Region.of(Location.region))
        case keys =>
          logger.info("Using AWS credentials from environment variables(ACCESS_KEY,SECRET_KEY)")
          val credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(keys._1, keys._2))
          S3AsyncClient.builder.region(Region.of(Location.region)).credentialsProvider(credentials)
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
