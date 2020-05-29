package etlflow

import java.net.URI
import java.util.concurrent.CompletableFuture
import etlflow.utils.AWS
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import scala.collection.JavaConverters._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{ListBucketsResponse, ListObjectsV2Request, ListObjectsV2Response}
import zio.{Has, IO, Task, ZIO, ZLayer}

package object aws {
  val aws_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  type S3Client = Has[S3Client.Service]
  object S3Client {
    trait Service {
      val s3: S3AsyncClient
    }
    val any: ZLayer[S3Client, Nothing, S3Client] =
      ZLayer.requires[S3Client]
    val live: ZLayer[S3AsyncClient, Nothing, S3Client] =
      ZLayer.fromFunction((curr: S3AsyncClient) => new S3Client.Service { val s3 = curr })
  }

  type S3Api = Has[S3Api.Service]
  object S3Api {
    trait Service {
      def listBuckets: Task[ListBucketsResponse]
      def lookupObject(buck: String, prefix: String, key: String): Task[Boolean]
      def listBucketObjects(buck: String, prefix: String): Task[ListObjectsV2Response]
    }
    val any: ZLayer[S3Api, Nothing, S3Api] = ZLayer.requires[S3Api]
    val live: ZLayer[S3Client, Throwable, S3Api] = ZLayer.fromService { deps: S3Client.Service =>
      new Service {
        def listBuckets: Task[ListBucketsResponse] =
          IO.effectAsync[Throwable, ListBucketsResponse](callback => processResponse(deps.s3.listBuckets, callback))
        def listBucketObjects(bucket: String, prefix: String): Task[ListObjectsV2Response] = for {
            resp <- IO.effect(
              deps.s3.listObjectsV2(
                ListObjectsV2Request.builder
                  .bucket(bucket)
                  .maxKeys(20)
                  .prefix(prefix)
                  .build
              )
            )
            list <- IO.effectAsync[Throwable, ListObjectsV2Response] { callback =>
              processResponse(
                resp,
                callback
              )
            }
          } yield list
        def lookupObject(bucket: String, prefix: String, key: String): Task[Boolean] = for {
            list    <- listBucketObjects(bucket, prefix)
            _       = aws_logger.info(s"Objects under bucket $bucket with prefix $prefix are \n" + list.contents().asScala.mkString("\n"))
            newKey  = prefix + "/" + key
            res     = list.contents.asScala.exists(_.key == newKey)
            _       = aws_logger.info(s"Object present: ${res.toString}")
          } yield res
        def processResponse[T](fut: CompletableFuture[T], callback: Task[T] => Unit): Unit = fut.handle[Unit] { (response, err) =>
            err match {
              case null => callback(IO.succeed(response))
              case ex => callback(IO.fail(ex))
            }
          }: Unit
      }
    }
  }

  def createClient(region: Region, endpointOverride: Option[String] = None, credentials: Option[AWS] = None): Task[S3AsyncClient] = {
    val ACCESS_KEY = sys.env.getOrElse("ACCESS_KEY", "NOT_SET_IN_ENV")
    val SECRET_KEY = sys.env.getOrElse("SECRET_KEY", "NOT_SET_IN_ENV")

    val initBuilder = credentials match {
      case Some(creds) =>
        aws_logger.info("Using AWS credentials from credentials passed in function")
        val credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(creds.access_key, creds.secret_key))
        S3AsyncClient.builder.region(region).credentialsProvider(credentials)
      case None => (ACCESS_KEY, SECRET_KEY) match {
        case ("NOT_SET_IN_ENV", "NOT_SET_IN_ENV") =>
          aws_logger.info("Using AWS credentials from local sdk")
          S3AsyncClient.builder.region(region)
        case keys =>
          aws_logger.info("Using AWS credentials from environment variables(ACCESS_KEY,SECRET_KEY)")
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
  def lookupObject(bucket: String, prefix: String, key: String): ZIO[S3Api, Throwable, Boolean] = ZIO.accessM(_.get.lookupObject(bucket,prefix,key))
}
