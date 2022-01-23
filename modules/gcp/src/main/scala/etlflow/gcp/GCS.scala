package etlflow.gcp

import com.google.api.gax.paging.Page
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Storage, StorageOptions}
import etlflow.model.Credential.GCP
import etlflow.utils.ApplicationLogger
import zio.{IO, Layer, Managed, Task, ZIO, ZLayer}
import java.io.FileInputStream
import java.nio.file.{FileSystems, Files, Path, Paths}
import scala.jdk.CollectionConverters._

object GCS extends ApplicationLogger {

  def getClient(credentials: Option[GCP]): Storage = {
    val env_path: String = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", "NOT_SET_IN_ENV")
    def getStorageClient(path: String) = {
      val credentials: GoogleCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(path))
      StorageOptions.newBuilder().setCredentials(credentials).build().getService
    }
    credentials match {
      case Some(creds) =>
        logger.info("Using GCP credentials from values passed in function")
        getStorageClient(creds.service_account_key_path)
      case None =>
        if (env_path == "NOT_SET_IN_ENV") {
          logger.info("Using GCP credentials from local sdk")
          StorageOptions.newBuilder().build().getService
        } else {
          logger.info("Using GCP credentials from environment variable GOOGLE_APPLICATION_CREDENTIALS")
          getStorageClient(env_path)
        }
    }
  }

  def live(credentials: Option[GCP] = None): Layer[Throwable, GCSEnv] = ZLayer.fromManaged {
    val acquire = IO.effect {
      getClient(credentials)
    }
    Managed.fromEffect(acquire).map { storage =>
      new GCSApi.Service {
        private def compareBlobs(
            src: List[Blob],
            src_path: String,
            target: List[Blob],
            target_path: String,
            overwrite: Boolean
        ): Task[Unit] = {
          val getName = (listOfBlob: List[Blob], pathToReplace: String) =>
            listOfBlob.map(_.getName.replace(pathToReplace, "").replace("/", ""))

          val sourceFileNames = getName(src, src_path)
          val targetFileNames = getName(target, target_path)

          val intersectCount = (sourceFileNames.intersect(targetFileNames)).size

          ZIO.fail(new Exception("File already exists")).unless(intersectCount == 0)
        }.unless(overwrite)
        override def listObjects(bucket: String, options: List[BlobListOption]): Task[Page[Blob]] = Task {
          storage.list(bucket, options: _*)
        }
        override def listObjects(bucket: String, prefix: String): Task[List[Blob]] = {
          val options: List[BlobListOption] = List(
            // BlobListOption.currentDirectory(),
            BlobListOption.prefix(prefix)
          )
          listObjects(bucket, options)
            .map(_.iterateAll().asScala)
            .map { blobs =>
              // if (blobs.nonEmpty) logger.info("Objects \n"+blobs.mkString("\n"))
              // else logger.info(s"No Objects found under gs://$bucket/$prefix")
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
              if (blobs.nonEmpty) logger.info("Objects \n" + blobs.mkString("\n"))
              else logger.info(s"No Objects found under gs://$bucket/$prefix")
              blobs.exists(_.getName == prefix + "/" + key)
            }
        }
        override def putObject(bucket: String, prefix: String, file: String): Task[Blob] = Task {
          val blobId   = BlobId.of(bucket, prefix)
          val blobInfo = BlobInfo.newBuilder(blobId).build
          storage.create(blobInfo, Files.readAllBytes(Paths.get(file)))
        }
        override def copyObjectsGCStoGCS(
            src_bucket: String,
            src_prefix: String,
            target_bucket: String,
            target_prefix: String,
            parallelism: Int,
            overwrite: Boolean
        ): Task[Unit] =
          for {
            src_blobs    <- listObjects(src_bucket, src_prefix)
            target_blobs <- listObjects(target_bucket, target_prefix)
            _            <- compareBlobs(src_blobs, src_prefix, target_blobs, target_prefix, overwrite)
            _ <- ZIO.foreachParN_(parallelism)(src_blobs)(blob =>
              Task {
                val target_path = (target_prefix + "/" + blob.getName.replace(src_prefix, "")).replaceAll("//+", "/")
                logger.info(s"Copying object from gs://$src_bucket/${blob.getName} to gs://$target_bucket/$target_path")
                blob.copyTo(target_bucket, target_path)
              }
            )
          } yield ()

        /** If target path ends with / -> that means directory in that case append file name to it "source_bucket":
          * "local", "source_path": "/path/to/dir/", "target_bucket" : "gcs-bucket", "target_path" : "/remote/path/" val
          * path = getTargetPath("/path/to/dir/file1.txt", "/path/to/dir/", "/remote/path/") path =
          * "/remote/path/file1.txt" Else it means it's a file -> directly return same "source_bucket": "local",
          * "source_path": "/path/to/file/file.txt", "target_bucket" : "gcs-bucket", "target_path" : "/path/file.txt"
          * val path = getTargetPath("/path/to/file/file.txt", "/path/to/file/file.txt", "/path/file.txt") path =
          * "/path/file.txt"
          */
        private def getTargetPath(fileName: Path, srcPath: String, targetPath: String): String = {
          if (targetPath.endsWith("/")) {
            val replaceableString =
              if (srcPath.endsWith("/"))
                srcPath // better approach -> new File(fileStore.sourcePath).isDirectory
              else {
                val splitFilePath = srcPath.split("/")
                splitFilePath.slice(0, splitFilePath.length - 1).mkString("/")
              }
            targetPath + fileName.toString.replace(replaceableString, "")
          } else targetPath
        }.replaceAll("//+", "/")
        private def listLocalFsObjects(path: String): Task[Iterator[Path]] = Task {
          val dir = FileSystems.getDefault.getPath(path)
          Files.walk(dir).iterator().asScala.filter(Files.isRegularFile(_))
        }
        override def copyObjectsLOCALtoGCS(
            src_path: String,
            target_bucket: String,
            target_prefix: String,
            parallelism: Int,
            overwrite: Boolean
        ): Task[Unit] =
          for {
            src_paths <- listLocalFsObjects(src_path)
            _ <- ZIO.foreachParN_(parallelism)(src_paths.toList)(path =>
              Task {
                val target_path = getTargetPath(path, src_path, target_prefix)
                val blobInfo    = BlobInfo.newBuilder(BlobId.of(target_bucket, target_path)).build
                logger.info(s"Copying object from local fs ${path.toString} to gs://$target_bucket/$target_path")
                storage.create(blobInfo, Files.readAllBytes(path))
              }
            )
          } yield ()
      }
    }
  }
}
