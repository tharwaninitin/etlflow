package etlflow.gcp

import java.io.FileInputStream
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.storage.{Storage, StorageOptions}
import etlflow.utils.Environment.GCP

object GCSClient {

  def apply(credentials: Option[GCP] = None): Storage = {
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


}
