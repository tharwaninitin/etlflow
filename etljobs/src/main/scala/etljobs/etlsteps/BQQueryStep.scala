package etljobs.etlsteps

import com.google.cloud.bigquery.BigQuery
import etljobs.bigquery.QueryApi
import scala.util.Try

case class BQQueryStep(name: String, query: String)(bq: => BigQuery) extends EtlStep[Unit, Unit] {
  def process(input_state : Unit): Try[Unit] = {
    Try {
      etl_logger.info("#"*100)
      etl_logger.info(s"Starting BQ Query Step: $name")
      etl_logger.info(s"Query: $query")
      QueryApi.executeQuery(bq, query)
      etl_logger.info("#"*100)
    }
  }
  override def getStepProperties(level: String): Map[String, String] = Map("query" -> query)
}


