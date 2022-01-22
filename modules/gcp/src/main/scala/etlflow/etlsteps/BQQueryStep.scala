package etlflow.etlsteps

import etlflow.gcp._
import etlflow.model.Credential.GCP
import zio.Task

class BQQueryStep private[etlflow] (
    val name: String,
    query: => String,
    credentials: Option[GCP] = None
) extends EtlStep[Unit] {

  final def process: Task[Unit] = {
    logger.info("#" * 100)
    val env = BQ.live(credentials)
    logger.info(s"Starting BQ Query Step: $name")
    logger.info(s"Query: $query")
    BQApi.executeQuery(query).provideLayer(env)
  }

  override def getStepProperties: Map[String, String] = Map("query" -> query)
}

object BQQueryStep {
  def apply(
      name: String,
      query: => String,
      credentials: Option[GCP] = None
  ): BQQueryStep =
    new BQQueryStep(name, query, credentials)
}
