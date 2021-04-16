package etlflow.etlsteps

import etlflow.gcp._
import etlflow.Credential.GCP
import etlflow.utils.LoggingLevel
import zio.Task

class BQQueryStep private[etlflow](
                                    val name: String,
                                    query: => String,
                                    credentials: Option[GCP] = None
                                  )
  extends EtlStep[Unit, Unit] {

  final def process(input: =>Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    val env = BQ.live(credentials)
    etl_logger.info(s"Starting BQ Query Step: $name")
    etl_logger.info(s"Query: $query")
    BQService.executeQuery(query).provideLayer(env)
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> query)
}

object BQQueryStep {
  def apply(
             name: String,
             query: => String,
             credentials: Option[GCP] = None
           ): BQQueryStep =
    new BQQueryStep(name, query, credentials)
}


