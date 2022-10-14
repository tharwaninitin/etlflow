package etlflow.task

import gcp4zio.bq._
import zio.RIO

case class BQQueryTask(name: String, query: String) extends EtlTask[BQEnv, Unit] {

  override protected def process: RIO[BQEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting BQ Query Task: $name")
    logger.info(s"Query: $query")
    BQApi.executeQuery(query)
  }

  override def getTaskProperties: Map[String, String] = Map("query" -> query)
}
