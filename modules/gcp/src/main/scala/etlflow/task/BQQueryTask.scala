package etlflow.task

import com.google.cloud.bigquery.Job
import gcp4zio.bq._
import zio.config.ConfigDescriptor
import zio.config.ConfigDescriptor._
import zio.{RIO, ZIO}

case class BQQueryTask(name: String, queries: List[String]) extends EtlTask[BQ, List[Job]] {

  override protected def process: RIO[BQ, List[Job]] = {
    logger.info("#" * 100)
    logger.info(s"Starting BQ Query Task: $name")
    ZIO.foreach(queries)(BQ.executeQuery)
  }

  override def getTaskProperties: Map[String, String] = Map.empty
}

object BQQueryTask {
  val config: ConfigDescriptor[BQQueryTask] =
    string("name")
      .zip(list("queries")(string))
      .to[BQQueryTask]
}
