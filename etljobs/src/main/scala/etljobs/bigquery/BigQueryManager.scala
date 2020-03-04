package etljobs.bigquery

import java.io.FileInputStream

import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import etljobs.utils.GlobalProperties
import org.apache.log4j.Logger

trait BigQueryManager {
  private val ic_logger = Logger.getLogger(getClass.getName)
  val global_properties: Option[GlobalProperties]
  ic_logger.info(f"======> Loaded BigQueryManager(${getClass.getName})")

  lazy val bq: BigQuery = createBigQuerySession(global_properties)

  def createBigQuerySession(gp: Option[GlobalProperties]): BigQuery = {
    gp match {
      case Some(ss) =>
        ic_logger.info(s"Job is running on env: ${ss.running_environment}")
        if (ss.running_environment == "gcp") BigQueryOptions.getDefaultInstance.getService
        else if (ss.running_environment == "aws" || ss.running_environment == "local")  {
          val credentials: GoogleCredentials  = ServiceAccountCredentials.fromStream(new FileInputStream(ss.gcp_project_key_name))
          BigQueryOptions.newBuilder().setCredentials(credentials).build().getService
        }
        else {
          throw new Exception("Exception occurred! Please provide correct value of property running_environment in loaddata.properties. Expected values are gcp or aws or local")
        }
      case None =>
        ic_logger.info(s"Job is running without GlobalProperties")
        BigQueryOptions.getDefaultInstance.getService
    }
  }
}
