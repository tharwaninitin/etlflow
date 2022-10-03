package etlflow.aws

import software.amazon.awssdk.services.s3.model._
import zio.stream.{Stream, ZStream}
import zio.{RIO, Task, ZIO}
import java.nio.file.Path

object S3Api {
  trait Service {
    def createBucket(name: String): Task[CreateBucketResponse]
    def listBuckets: Task[List[Bucket]]
    def listObjects(bucket: String, key: String, maxKeys: Int): Task[ListObjectsV2Response]
    def lookupObject(bucket: String, key: String): Task[Boolean]
    def putObject(bucket: String, key: String, file: Path, overwrite: Boolean): Task[PutObjectResponse]
    def putObject[R](bucket: String, key: String, content: ZStream[R, Throwable, Byte], contentLength: Long): RIO[R, Unit]
    def getObject(bucket: String, key: String, file: Path): Task[GetObjectResponse]
    def getObject(bucket: String, key: String): Stream[Throwable, Byte]
    def delObject(bucket: String, key: String): Task[DeleteObjectResponse]
  }

  def createBucket(name: String): RIO[S3Env, CreateBucketResponse] = ZIO.environmentWithZIO[S3Env](_.get.createBucket(name))
  def listBuckets: RIO[S3Env, List[Bucket]]                        = ZIO.environmentWithZIO[S3Env](_.get.listBuckets)
  def lookupObject(bucket: String, key: String): RIO[S3Env, Boolean] =
    ZIO.environmentWithZIO[S3Env](_.get.lookupObject(bucket, key))
  def listObjects(bucket: String, key: String, maxKeys: Int): RIO[S3Env, ListObjectsV2Response] =
    ZIO.environmentWithZIO[S3Env](_.get.listObjects(bucket, key, maxKeys))
  def putObject[R](
      bucket: String,
      key: String,
      content: ZStream[R, Throwable, Byte],
      contentLength: Long
  ): RIO[R with S3Env, Unit] =
    ZIO.environmentWithZIO[R with S3Env](_.get.putObject[R](bucket, key, content, contentLength))
  def putObject(bucket: String, key: String, file: Path, overwrite: Boolean): RIO[S3Env, PutObjectResponse] =
    ZIO.environmentWithZIO[S3Env](_.get.putObject(bucket, key, file, overwrite))
  def getObject(bucket: String, key: String, file: Path): RIO[S3Env, GetObjectResponse] =
    ZIO.environmentWithZIO[S3Env](_.get.getObject(bucket, key, file))
  def getObject(bucket: String, key: String): ZStream[S3Env, Throwable, Byte] =
    ZStream.environmentWithStream[S3Env](_.get.getObject(bucket, key))
  def delObject(bucket: String, key: String): RIO[S3Env, DeleteObjectResponse] =
    ZIO.environmentWithZIO[S3Env](_.get.delObject(bucket, key))
}
