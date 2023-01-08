package etlflow.task

import com.google.cloud.dataproc.v1.Job
import gcp4zio.dp._
import zio.RIO

case class DPHiveJobTask(name: String, query: String) extends EtlTask[DPJob, Job] {

  override protected def process: RIO[DPJob, Job] = {
    logger.info("#" * 100)
    logger.info(s"Starting Hive Dataproc Job: $name")
    DPJob.executeHiveJob(query)
  }

  override def getTaskProperties: Map[String, String] = Map("query" -> query)
}
