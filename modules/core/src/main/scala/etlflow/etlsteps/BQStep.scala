package etlflow.etlsteps

import java.io.FileInputStream
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}

trait BQStep extends EtlStep[Unit,Unit] {

  val gcp_credential_file_path: Option[String]

  private val env_path: String = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", "NOT_SET_IN_ENV")

  private def getBQ(path: String) = {
    val credentials: GoogleCredentials  = ServiceAccountCredentials.fromStream(new FileInputStream(path))
    BigQueryOptions.newBuilder().setCredentials(credentials).build().getService
  }

  lazy val bq: BigQuery = gcp_credential_file_path match {
    case Some(file_path) => getBQ(file_path)
    case None =>
      if (env_path == "NOT_SET_IN_ENV")
        BigQueryOptions.getDefaultInstance.getService
      else
        getBQ(env_path)
  }
}
