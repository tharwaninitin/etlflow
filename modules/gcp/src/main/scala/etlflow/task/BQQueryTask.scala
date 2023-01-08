package etlflow.task

import gcp4zio.bq._
import zio.RIO

case class BQQueryTask(name: String, query: String) extends EtlTask[BQ, Unit] {

  override protected def process: RIO[BQ, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting BQ Query Task: $name")
    logger.info(s"Query: $query")
    BQ.executeQuery(query)
  }

  override def getTaskProperties: Map[String, String] = Map.empty // TODO Sanitize query before using this Map("query" -> query)
}
