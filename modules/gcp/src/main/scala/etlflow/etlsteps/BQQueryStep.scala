package etlflow.etlsteps

import etlflow.gcp._
import zio.RIO

class BQQueryStep private[etlflow] (val name: String, query: => String) extends EtlStep[BQEnv, Unit] {

  final def process: RIO[BQEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting BQ Query Step: $name")
    logger.info(s"Query: $query")
    BQApi.executeQuery(query)
  }

  override def getStepProperties: Map[String, String] = Map("query" -> query)
}

object BQQueryStep {
  def apply(name: String, query: => String): BQQueryStep = new BQQueryStep(name, query)
}
