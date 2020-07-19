package etlflow.aws

import java.nio.file.Paths
import java.util.concurrent.CompletableFuture
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._
import zio.{IO, Task, ZLayer}

import scala.collection.JavaConverters._

object S3Impl {
  val live: ZLayer[S3AsyncClient, Throwable, S3Api] = ZLayer.fromFunction{s3: S3AsyncClient =>
    new Service {
      def listBuckets: Task[ListBucketsResponse] =
        IO.effectAsync[Throwable, ListBucketsResponse](callback => processResponse(s3.listBuckets, callback))
      def listBucketObjects(bucket: String, prefix: String, maxKeys: Int = 20): Task[ListObjectsV2Response] = for {
        resp <- IO.effect(
          s3.listObjectsV2(
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
          s3.putObject(PutObjectRequest.builder.bucket(bucket).key(key).build, Paths.get(file)),
          callback
        )
      }
      def getObject(bucket: String, key: String, file: String): Task[GetObjectResponse] = IO.effectAsync[Throwable, GetObjectResponse] { callback =>
        processResponse(
          s3.getObject(GetObjectRequest.builder.bucket(bucket).key(key).build, Paths.get(file)),
          callback
        )
      }
      def delObject(bucket: String, key: String): Task[DeleteObjectResponse] = IO.effectAsync[Throwable, DeleteObjectResponse] { callback =>
        processResponse(
          s3.deleteObject(DeleteObjectRequest.builder.bucket(bucket).key(key).build),
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
