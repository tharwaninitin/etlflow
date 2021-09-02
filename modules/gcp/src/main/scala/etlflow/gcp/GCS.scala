package etlflow.gcp

import java.io.FileInputStream
import java.nio.file.{Files, Paths}
import com.google.api.gax.paging.Page
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Storage, StorageOptions}
import etlflow.schema.Credential.GCP
import etlflow.schema.Location
import etlflow.utils.ApplicationLogger
import zio.{IO, Layer, Managed, Task, ZLayer}
import scala.jdk.CollectionConverters._

private[etlflow] object GCS extends ApplicationLogger{

  def getClient(location: Location.GCS): Storage = {
    val env_path: String = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", "NOT_SET_IN_ENV")
    def getStorageClient(path: String) = {
      val credentials: GoogleCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(path))
      StorageOptions.newBuilder().setCredentials(credentials).build().getService
    }
    location.credentials match {
      case Some(creds) =>
        logger.info("Using GCP credentials from values passed in function")
        getStorageClient(creds.service_account_key_path)
      case None =>
        if (env_path == "NOT_SET_IN_ENV") {
          logger.info("Using GCP credentials from local sdk")
          StorageOptions.newBuilder().build().getService
        }
        else {
          logger.info("Using GCP credentials from environment variable GOOGLE_APPLICATION_CREDENTIALS")
          getStorageClient(env_path)
        }
    }
  }

  def live(credentials: Option[GCP] = None): Layer[Throwable, GCSService] = ZLayer.fromManaged {
    val acquire = IO.effect{
      getClient(Location.GCS("","",credentials))
    }
    Managed.fromEffect(acquire).map { storage =>
      new GCSService.Service {
        override def listObjects(bucket: String, options: List[BlobListOption]): Task[Page[Blob]] = Task {
          storage.list(bucket, options: _*)
        }
        override def listObjects(bucket: String, prefix: String): Task[List[Blob]] = {
          val options: List[BlobListOption] = List(
            BlobListOption.currentDirectory(),
            BlobListOption.prefix(prefix)
          )
          listObjects(bucket, options)
            .map(_.iterateAll().asScala)
            .map { blobs =>
              if (blobs.nonEmpty) logger.info("Objects \n"+blobs.mkString("\n"))
              else logger.info(s"No Objects found under gs://$bucket/$prefix")
              blobs.toList
            }
        }
        override def lookupObject(bucket: String, prefix: String, key: String): Task[Boolean] = {
          val options: List[BlobListOption] = List(
            BlobListOption.prefix(prefix)
          )
          listObjects(bucket, options)
            .map(_.iterateAll().asScala)
            .map { blobs =>
              if (blobs.nonEmpty) logger.info("Objects \n"+blobs.mkString("\n"))
              else logger.info(s"No Objects found under gs://$bucket/$prefix")
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
