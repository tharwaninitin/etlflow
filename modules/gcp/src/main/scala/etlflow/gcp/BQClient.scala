package etlflow.gcp

import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import etlflow.model.Credential.GCP
import java.io.FileInputStream

object BQClient {

  private def getBQ(path: String): BigQuery = {
    val credentials: GoogleCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(path))
    BigQueryOptions.newBuilder().setCredentials(credentials).build().getService
  }

  def apply(credentials: Option[GCP] = None): BigQuery = {
    val env_path: String = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", "NOT_SET_IN_ENV")
    credentials match {
      case Some(creds) =>
        logger.info("Using GCP credentials from values passed in function")
        getBQ(creds.service_account_key_path)
      case None =>
        if (env_path == "NOT_SET_IN_ENV") {
          logger.info("Using GCP credentials from local sdk")
          BigQueryOptions.getDefaultInstance.getService
        } else {
          logger.info("Using GCP credentials from environment variable GOOGLE_APPLICATION_CREDENTIALS")
          getBQ(env_path)
        }
    }
  }
}
