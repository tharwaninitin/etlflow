package etlflow.etlsteps

import gcp4zio._
import zio.RIO

case class BQQueryStep(name: String, query: String) extends EtlStep[Unit] {
  override protected type R = BQEnv

  override protected def process: RIO[BQEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting BQ Query Step: $name")
    logger.info(s"Query: $query")
    BQApi.executeQuery(query)
  }

  override def getStepProperties: Map[String, String] = Map("query" -> query)
}
