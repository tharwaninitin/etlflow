package etlflow.task

import gcp4zio._
import zio.RIO

case class BQQueryTask(name: String, query: String) extends EtlTaskZIO[BQEnv, Unit] {

  override protected def processZio: RIO[BQEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting BQ Query Step: $name")
    logger.info(s"Query: $query")
    BQApi.executeQuery(query)
  }

  override def getStepProperties: Map[String, String] = Map("query" -> query)
}
