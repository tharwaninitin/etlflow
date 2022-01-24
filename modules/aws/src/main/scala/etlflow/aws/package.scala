package etlflow

import etlflow.model.Credential
import etlflow.model.Credential.AWS
import etlflow.utils.ApplicationLogger
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._
import zio.{Has, Task, ZIO}
import java.net.URI

package object aws extends ApplicationLogger {

  case class S3(bucket: String, location: String, region: String, credentials: Option[Credential.AWS] = None)

  type S3Api = Has[Service]
  private[etlflow] trait Service {
    def listBuckets: Task[ListBucketsResponse]
    def lookupObject(bucket: String, prefix: String, key: String): Task[Boolean]
    def listBucketObjects(bucket: String, prefix: String, maxKeys: Int): Task[ListObjectsV2Response]
    def putObject(bucket: String, key: String, file: String): Task[PutObjectResponse]
    def getObject(bucket: String, key: String, file: String): Task[GetObjectResponse]
    def delObject(bucket: String, key: String): Task[DeleteObjectResponse]
  }
  private[etlflow] object S3Api {
    lazy val env = S3Impl.live
    def createClient(
        region: Region,
        endpointOverride: Option[String] = None,
        credentials: Option[AWS] = None
    ): Task[S3AsyncClient] = {
      val ACCESS_KEY = sys.env.getOrElse("ACCESS_KEY", "NOT_SET_IN_ENV")
      val SECRET_KEY = sys.env.getOrElse("SECRET_KEY", "NOT_SET_IN_ENV")

      val initBuilder = credentials match {
        case Some(creds) =>
          logger.info("Using AWS credentials from credentials passed in function")
          val credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(creds.access_key, creds.secret_key))
          S3AsyncClient.builder.region(region).credentialsProvider(credentials)
        case None =>
          (ACCESS_KEY, SECRET_KEY) match {
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

      Task(client)
    }
    def listBuckets: ZIO[S3AsyncClient, Throwable, ListBucketsResponse] =
      ZIO.accessM[S3Api](_.get.listBuckets).provideLayer(env)
    def lookupObject(bucket: String, prefix: String, key: String): ZIO[S3AsyncClient, Throwable, Boolean] =
      ZIO.accessM[S3Api](_.get.lookupObject(bucket, prefix, key)).provideLayer(env)
    def listBucketObjects(bucket: String, prefix: String, maxKeys: Int): ZIO[S3AsyncClient, Throwable, ListObjectsV2Response] =
      ZIO.accessM[S3Api](_.get.listBucketObjects(bucket, prefix, maxKeys)).provideLayer(env)
    def putObject(bucket: String, key: String, file: String): ZIO[S3AsyncClient, Throwable, PutObjectResponse] =
      ZIO.accessM[S3Api](_.get.putObject(bucket, key, file)).provideLayer(env)
    def getObject(bucket: String, key: String, file: String): ZIO[S3AsyncClient, Throwable, GetObjectResponse] =
      ZIO.accessM[S3Api](_.get.getObject(bucket, key, file)).provideLayer(env)
    def delObject(bucket: String, key: String): ZIO[S3AsyncClient, Throwable, DeleteObjectResponse] =
      ZIO.accessM[S3Api](_.get.delObject(bucket, key)).provideLayer(env)
  }
}
