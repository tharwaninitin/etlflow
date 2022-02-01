package etlflow.gcp

import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.storage.{Storage, StorageOptions}
import etlflow.model.Credential.GCP
import java.io.FileInputStream

object GCSClient {

  private def getStorage(path: String): Storage = {
    val credentials: GoogleCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(path))
    StorageOptions.newBuilder().setCredentials(credentials).build().getService
  }

  def apply(credentials: Option[GCP]): Storage = {
    val env_path: String = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", "NOT_SET_IN_ENV")

    credentials match {
      case Some(creds) =>
        logger.info("Using GCP credentials from values passed in function")
        getStorage(creds.service_account_key_path)
      case None =>
        if (env_path == "NOT_SET_IN_ENV") {
          logger.info("Using GCP credentials from local sdk")
          StorageOptions.newBuilder().build().getService
        } else {
          logger.info("Using GCP credentials from environment variable GOOGLE_APPLICATION_CREDENTIALS")
          getStorage(env_path)
        }
    }
  }
}
