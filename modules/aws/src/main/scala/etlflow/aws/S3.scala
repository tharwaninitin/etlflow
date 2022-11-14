package etlflow.aws

import etlflow.model.Credential.AWS
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model._
import zio.stream.{Stream, ZStream}
import zio.{RIO, Task, TaskLayer, ZIO, ZLayer}
import java.nio.file.Path

trait S3 {
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

object S3 {
  def createBucket(name: String): RIO[S3, CreateBucketResponse] = ZIO.environmentWithZIO[S3](_.get.createBucket(name))
  def listBuckets: RIO[S3, List[Bucket]]                        = ZIO.environmentWithZIO[S3](_.get.listBuckets)
  def lookupObject(bucket: String, key: String): RIO[S3, Boolean] =
    ZIO.environmentWithZIO[S3](_.get.lookupObject(bucket, key))
  def listObjects(bucket: String, key: String, maxKeys: Int): RIO[S3, ListObjectsV2Response] =
    ZIO.environmentWithZIO[S3](_.get.listObjects(bucket, key, maxKeys))
  def putObject[R](
      bucket: String,
      key: String,
      content: ZStream[R, Throwable, Byte],
      contentLength: Long
  ): RIO[R with S3, Unit] =
    ZIO.environmentWithZIO[S3](_.get.putObject[R](bucket, key, content, contentLength))
  def putObject(bucket: String, key: String, file: Path, overwrite: Boolean): RIO[S3, PutObjectResponse] =
    ZIO.environmentWithZIO[S3](_.get.putObject(bucket, key, file, overwrite))
  def getObject(bucket: String, key: String, file: Path): RIO[S3, GetObjectResponse] =
    ZIO.environmentWithZIO[S3](_.get.getObject(bucket, key, file))
  def getObject(bucket: String, key: String): ZStream[S3, Throwable, Byte] =
    ZStream.environmentWithStream[S3](_.get.getObject(bucket, key))
  def delObject(bucket: String, key: String): RIO[S3, DeleteObjectResponse] =
    ZIO.environmentWithZIO[S3](_.get.delObject(bucket, key))
  def live(region: Region, credentials: Option[AWS] = None, endpointOverride: Option[String] = None): TaskLayer[S3] =
    ZLayer.scoped(ZIO.attempt(S3Client(region, credentials, endpointOverride)).map(s3 => S3Impl(s3)))
}
