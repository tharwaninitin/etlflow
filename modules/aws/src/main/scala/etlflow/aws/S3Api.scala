package etlflow.aws

import software.amazon.awssdk.services.s3.model._
import zio.stream.{Stream, ZStream}
import zio.{RIO, Task, ZIO}

object S3Api {
  trait Service {
    def listBuckets: Task[ListBucketsResponse]
    def lookupObject(bucket: String, prefix: String, key: String): Task[Boolean]
    def listBucketObjects(bucket: String, prefix: String, maxKeys: Int): Task[ListObjectsV2Response]
    def putObject(bucket: String, key: String, file: String): Task[PutObjectResponse]
    def getObject(bucket: String, key: String, file: String): Task[GetObjectResponse]
    def getObject(bucket: String, key: String): Stream[S3Exception, Byte]
    def delObject(bucket: String, key: String): Task[DeleteObjectResponse]
  }

  def listBuckets: RIO[S3Env, ListBucketsResponse] = ZIO.accessM[S3Env](_.get.listBuckets)
  def lookupObject(bucket: String, prefix: String, key: String): RIO[S3Env, Boolean] =
    ZIO.accessM[S3Env](_.get.lookupObject(bucket, prefix, key))
  def listBucketObjects(bucket: String, prefix: String, maxKeys: Int): RIO[S3Env, ListObjectsV2Response] =
    ZIO.accessM[S3Env](_.get.listBucketObjects(bucket, prefix, maxKeys))
  def putObject(bucket: String, key: String, file: String): RIO[S3Env, PutObjectResponse] =
    ZIO.accessM[S3Env](_.get.putObject(bucket, key, file))
  def getObject(bucket: String, key: String, file: String): RIO[S3Env, GetObjectResponse] =
    ZIO.accessM[S3Env](_.get.getObject(bucket, key, file))
  def getObject(bucket: String, key: String): ZStream[S3Env, S3Exception, Byte] =
    ZStream.accessStream[S3Env](_.get.getObject(bucket, key))
  def delObject(bucket: String, key: String): RIO[S3Env, DeleteObjectResponse] =
    ZIO.accessM[S3Env](_.get.delObject(bucket, key))
}
