package etljobs.etlsteps

import com.google.cloud.bigquery.BigQuery
import etljobs.bigquery.QueryApi
import zio.Task

case class BQQueryStep private[etljobs] (name: String, query: String) extends EtlStep[BigQuery,Unit] {

  def process(bq : BigQuery): Task[Unit] = Task {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting BQ Query Step: $name")
    etl_logger.info(s"Query: $query")
    QueryApi.executeQuery(bq, query)
    etl_logger.info("#"*100)
  }

  override def getStepProperties(level: String): Map[String, String] = Map("query" -> query)
}


