package etlflow.task

import com.google.cloud.bigquery.Job
import gcp4zio.bq._
import zio.RIO

case class BQQueryTask(name: String, query: String) extends EtlTask[BQ, Job] {

  override protected def process: RIO[BQ, Job] = {
    logger.info("#" * 100)
    logger.info(s"Starting BQ Query Task: $name")
    logger.info(s"Query: $query")
    BQ.executeQuery(query)
  }

  override def getTaskProperties: Map[String, String] = Map.empty // TODO Sanitize query before using this Map("query" -> query)
}
