package etlflow

import java.net.URI
import java.nio.file.Paths
import java.util.concurrent.CompletableFuture

import etlflow.utils.AWS
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider

import scala.collection.JavaConverters._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{DeleteObjectRequest, DeleteObjectResponse, GetObjectRequest, GetObjectResponse, ListBucketsResponse, ListObjectsV2Request, ListObjectsV2Response, PutObjectRequest, PutObjectResponse}
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
      def lookupObject(bucket: String, prefix: String, key: String): Task[Boolean]
      def listBucketObjects(bucket: String, prefix: String, maxKeys: Int): Task[ListObjectsV2Response]
      def putObject(bucket: String, key: String, file: String): Task[PutObjectResponse]
      def getObject(bucket: String, key: String, file: String): Task[GetObjectResponse]
      def delObject(bucket: String, key: String): Task[DeleteObjectResponse]
    }
    val any: ZLayer[S3Api, Nothing, S3Api] = ZLayer.requires[S3Api]
    val live: ZLayer[S3Client, Throwable, S3Api] = ZLayer.fromService { deps: S3Client.Service =>
      new Service {
        def listBuckets: Task[ListBucketsResponse] =
          IO.effectAsync[Throwable, ListBucketsResponse](callback => processResponse(deps.s3.listBuckets, callback))
        def listBucketObjects(bucket: String, prefix: String, maxKeys: Int = 20): Task[ListObjectsV2Response] = for {
            resp <- IO.effect(
              deps.s3.listObjectsV2(
                ListObjectsV2Request.builder
                  .bucket(bucket)
                  .maxKeys(maxKeys)
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
            list    <- listBucketObjects(bucket, prefix, Integer.MAX_VALUE)
            _       = aws_logger.info{
                      if (list.contents().asScala.nonEmpty)
                        s"Objects under bucket $bucket with prefix $prefix are \n" + list.contents().asScala.mkString("\n")
                      else s"No objects under bucket $bucket with prefix $prefix"
                      }
            newKey  = prefix + "/" + key
            res     = list.contents.asScala.exists(_.key == newKey)
          } yield res
        def putObject(bucket: String, key: String, file: String): Task[PutObjectResponse] = IO.effectAsync[Throwable, PutObjectResponse] { callback =>
            processResponse(
              deps.s3.putObject(PutObjectRequest.builder.bucket(bucket).key(key).build, Paths.get(file)),
              callback
            )
          }
        def getObject(bucket: String, key: String, file: String): Task[GetObjectResponse] = IO.effectAsync[Throwable, GetObjectResponse] { callback =>
            processResponse(
              deps.s3.getObject(GetObjectRequest.builder.bucket(bucket).key(key).build, Paths.get(file)),
              callback
            )
          }
        def delObject(bucket: String, key: String): Task[DeleteObjectResponse] = IO.effectAsync[Throwable, DeleteObjectResponse] { callback =>
            processResponse(
              deps.s3.deleteObject(DeleteObjectRequest.builder.bucket(bucket).key(key).build),
              callback
            )
          }
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
        case (access_key, secret_key) if access_key == "NOT_SET_IN_ENV" || secret_key == "NOT_SET_IN_ENV" =>
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
  def putObject(bucket: String, key: String, file: String): ZIO[S3Api, Throwable, PutObjectResponse] = ZIO.accessM(_.get.putObject(bucket,key,file))
  def lookupObject(bucket: String, prefix: String, key: String): ZIO[S3Api, Throwable, Boolean] = ZIO.accessM(_.get.lookupObject(bucket,prefix,key))
}
