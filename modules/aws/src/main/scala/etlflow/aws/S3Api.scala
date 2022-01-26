package etlflow.aws

import software.amazon.awssdk.services.s3.model._
import zio.{RIO, ZIO}

object S3Api {
  trait Service[F[_]] {
    def listBuckets: F[ListBucketsResponse]
    def lookupObject(bucket: String, prefix: String, key: String): F[Boolean]
    def listBucketObjects(bucket: String, prefix: String, maxKeys: Int): F[ListObjectsV2Response]
    def putObject(bucket: String, key: String, file: String): F[PutObjectResponse]
    def getObject(bucket: String, key: String, file: String): F[GetObjectResponse]
    def delObject(bucket: String, key: String): F[DeleteObjectResponse]
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
  def delObject(bucket: String, key: String): RIO[S3Env, DeleteObjectResponse] =
    ZIO.accessM[S3Env](_.get.delObject(bucket, key))
}
