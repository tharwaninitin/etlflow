package etlflow.task

import com.google.cloud.bigquery.Job
import gcp4zio.bq._
import zio.config.ConfigDescriptor
import zio.config.ConfigDescriptor._
import zio.{RIO, ZIO}

case class BQQueryListTask(name: String, queries: List[String]) extends EtlTask[BQ, List[Job]] {

  override def getTaskProperties: Map[String, String] = Map.empty // TODO Sanitize query before using this Map("query" -> query)

  override protected def process: RIO[BQ, List[Job]] = {
    logger.info("#" * 100)
    logger.info(s"Starting BQ Query Task: $name")
    ZIO.foreach(queries)(run)
  }

  private def run(query: String): RIO[BQ, Job] = {
    logger.info(s"Query: $query")
    BQ.executeQuery(query)
  }
}

object BQQueryListTask {
  val config: ConfigDescriptor[BQQueryListTask] =
    string("name")
      .zip(list("queries")(string))
      .to[BQQueryListTask]
}
