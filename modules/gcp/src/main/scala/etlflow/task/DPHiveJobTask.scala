package etlflow.task

import com.google.cloud.dataproc.v1.Job
import gcp4zio._
import zio.RIO

case class DPHiveJobTask(name: String, query: String, cluster: String, project: String, region: String)
    extends EtlTask[DPJobEnv, Job] {

  override protected def process: RIO[DPJobEnv, Job] = {
    logger.info("#" * 100)
    logger.info(s"Starting Hive Dataproc Job: $name")
    DPJobApi.executeHiveJob(query, cluster, project, region)
  }

  override def getTaskProperties: Map[String, String] = Map(
    "query"   -> query,
    "cluster" -> cluster,
    "project" -> project,
    "region"  -> region
  )
}
