package etlflow.task

import com.google.cloud.dataproc.v1.Job
import gcp4zio.dp._
import zio.RIO

case class DPHiveJobTask(name: String, query: String, cluster: String, project: String, region: String)
    extends EtlTask[DPJob, Job] {

  override protected def process: RIO[DPJob, Job] = {
    logger.info("#" * 100)
    logger.info(s"Starting Hive Dataproc Job: $name")
    for {
      job <- DPJob.submitHiveJob(query, cluster, project, region)
      _   <- DPJob.trackJobProgress(project, region, job)
    } yield job

  }

  override def getTaskProperties: Map[String, String] = Map(
    "query"   -> query,
    "cluster" -> cluster,
    "project" -> project,
    "region"  -> region
  )
}
