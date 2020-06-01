package etlflow.etlsteps

import java.io.FileInputStream
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import etlflow.utils.GCP

trait BQStep extends EtlStep[Unit,Unit] {

  val credentials: Option[GCP]

  private val env_path: String = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", "NOT_SET_IN_ENV")

  private def getBQ(path: String) = {
    val credentials: GoogleCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(path))
    BigQueryOptions.newBuilder().setCredentials(credentials).build().getService
  }

  lazy val bq: BigQuery = credentials match {
    case Some(creds) =>
      etl_logger.info("Using GCP credentials from values passed in function")
      getBQ(creds.service_account_key_path)
    case None =>
      if (env_path == "NOT_SET_IN_ENV") {
        etl_logger.info("Using GCP credentials from local sdk")
        BigQueryOptions.getDefaultInstance.getService
      }
      else {
        etl_logger.info("Using GCP credentials from environment variable GOOGLE_APPLICATION_CREDENTIALS")
        getBQ(env_path)
      }
  }
}
