package etlflow

import java.io.FileInputStream

import com.google.api.gax.paging.Page
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import java.nio.file.Files
import java.nio.file.Paths
import scala.collection.JavaConverters._
import com.google.cloud.storage.{Blob, BlobInfo, StorageOptions}
import com.google.cloud.storage.Storage.{BlobListOption, BlobTargetOption}
import etlflow.utils.GCP
import org.slf4j.{Logger, LoggerFactory}
import zio.{Has, IO, Layer, Managed, RIO, Task, ZIO, ZLayer}

package object gcp {
  val gcp_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  type GCSStorage = Has[GCSStorage.Service]

  object GCSStorage {
    trait Service {
      def listObjects(bucket: String, options: List[BlobListOption]): ZIO[GCSStorage, Throwable, Page[Blob]]
      def lookupObject(bucket: String, prefix: String, key: String): ZIO[GCSStorage, Throwable, Boolean]
      def putObject(bucket: String, key: String, file: String): ZIO[GCSStorage, Throwable, Blob]
    }
    def live(credentials: Option[GCP] = None): Layer[Throwable, GCSStorage] = ZLayer.fromManaged {
        val acquire = IO.effect{
          val env_path: String = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", "NOT_SET_IN_ENV")
          def getStorageClient(path: String) = {
            val credentials: GoogleCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(path))
            StorageOptions.newBuilder().setCredentials(credentials).build().getService
          }
          credentials match {
            case Some(creds) =>
              gcp_logger.info("Using GCP credentials from values passed in function")
              getStorageClient(creds.service_account_key_path)
            case None =>
              if (env_path == "NOT_SET_IN_ENV") {
                gcp_logger.info("Using GCP credentials from local sdk")
                StorageOptions.newBuilder().build().getService
              }
              else {
                gcp_logger.info("Using GCP credentials from environment variable GOOGLE_APPLICATION_CREDENTIALS")
                getStorageClient(env_path)
              }
          }
        }
        Managed.fromEffect(acquire).map { storage =>
          new Service {
            override def listObjects(bucket: String, options: List[BlobListOption]): Task[Page[Blob]] = Task {
              storage.list(bucket, options: _*)
            }
            override def lookupObject(bucket: String, prefix: String, key: String): Task[Boolean] = {
              val options: List[BlobListOption] = List(
                BlobListOption.prefix(prefix)
              )
              listObjects(bucket, options)
                .map(_.iterateAll().asScala)
                .map { blobs =>
                  if (blobs.nonEmpty) gcp_logger.info("Objects \n"+blobs.mkString("\n"))
                  else gcp_logger.info(s"No Objects found under gs://$bucket/$prefix")
                  blobs.exists(_.getName == prefix+"/"+key)
                }
            }
            override def putObject(bucket: String, key: String, file: String): Task[Blob] = Task{
              val blobId = BlobId.of(bucket, key)
              val blobInfo = BlobInfo.newBuilder(blobId).build
              storage.create(blobInfo, Files.readAllBytes(Paths.get(file)))
            }
          }
        }
      }
  }

  def putObject(bucket: String, key: String, file: String): ZIO[GCSStorage, Throwable, Blob] =
    ZIO.accessM(_.get.putObject(bucket,key,file))

  def lookupObject(bucket: String, prefix: String, key: String): ZIO[GCSStorage, Throwable, Boolean] =
    ZIO.accessM(_.get.lookupObject(bucket,prefix,key))

  def listObjects(bucket: String, options: List[BlobListOption]): ZIO[GCSStorage, Throwable, Page[Blob]] =
    ZIO.accessM(_.get.listObjects(bucket,options))
}
