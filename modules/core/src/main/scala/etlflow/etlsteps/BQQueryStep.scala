package etlflow.etlsteps

import etlflow.bigquery.QueryApi
import zio.Task

case class BQQueryStep private[etlflow](
                 name: String
                 , query: String
                 , gcp_credential_file_path: Option[String] = None
           )
  extends BQStep {

  final def process(input: =>Unit): Task[Unit] = Task {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting BQ Query Step: $name")
    etl_logger.info(s"Query: $query")
    QueryApi.executeQuery(bq, query)
    etl_logger.info("#"*100)
  }

  override def getStepProperties(level: String): Map[String, String] = Map("query" -> query)
}


