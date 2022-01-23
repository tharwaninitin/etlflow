package etlflow.gcp

import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage.BlobListOption
import zio.{Task, ZIO}

object GCSApi {
  trait Service {
    def putObject(bucket: String, prefix: String, file: String): Task[Blob]
    def lookupObject(bucket: String, prefix: String, key: String): Task[Boolean]
    def listObjects(bucket: String, options: List[BlobListOption]): Task[Page[Blob]]
    def listObjects(bucket: String, prefix: String): Task[List[Blob]]
    def copyObjectsGCStoGCS(
        src_bucket: String,
        src_prefix: String,
        target_bucket: String,
        target_prefix: String,
        parallelism: Int,
        overwrite: Boolean
    ): Task[Unit]
    def copyObjectsLOCALtoGCS(
        src_path: String,
        target_bucket: String,
        target_prefix: String,
        parallelism: Int,
        overwrite: Boolean
    ): Task[Unit]
  }
  def putObject(bucket: String, prefix: String, file: String): ZIO[GCSEnv, Throwable, Blob] =
    ZIO.accessM(_.get.putObject(bucket, prefix, file))
  def lookupObject(bucket: String, prefix: String, key: String): ZIO[GCSEnv, Throwable, Boolean] =
    ZIO.accessM(_.get.lookupObject(bucket, prefix, key))
  def listObjects(bucket: String, options: List[BlobListOption]): ZIO[GCSEnv, Throwable, Page[Blob]] =
    ZIO.accessM(_.get.listObjects(bucket, options))
  def listObjects(bucket: String, prefix: String): ZIO[GCSEnv, Throwable, List[Blob]] =
    ZIO.accessM(_.get.listObjects(bucket, prefix))
  def copyObjectsGCStoGCS(
      src_bucket: String,
      src_prefix: String,
      target_bucket: String,
      target_prefix: String,
      parallelism: Int,
      overwrite: Boolean
  ): ZIO[GCSEnv, Throwable, Unit] =
    ZIO.accessM(_.get.copyObjectsGCStoGCS(src_bucket, src_prefix, target_bucket, target_prefix, parallelism, overwrite))
  def copyObjectsLOCALtoGCS(
      src_path: String,
      target_bucket: String,
      target_prefix: String,
      parallelism: Int,
      overwrite: Boolean
  ): ZIO[GCSEnv, Throwable, Unit] =
    ZIO.accessM(_.get.copyObjectsLOCALtoGCS(src_path, target_bucket, target_prefix, parallelism, overwrite))
}
