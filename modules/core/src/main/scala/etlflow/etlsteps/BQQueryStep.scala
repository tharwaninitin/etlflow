package etlflow.etlsteps

import etlflow.bigquery.QueryApi
import etlflow.utils.GCP
import zio.Task

class BQQueryStep private[etlflow](
                 val name: String
                 , query: => String
                 ,val credentials: Option[GCP] = None
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

object BQQueryStep {
  def apply(
             name: String,
             query: => String,
             credentials: Option[GCP] = None
           ): BQQueryStep =
    new BQQueryStep(name, query, credentials)
}


