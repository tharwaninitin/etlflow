package etlflow.etlsteps

import com.google.cloud.dataproc.v1.Job
import gcp4zio._
import zio.RIO

case class DPSparkJobStep(
    name: String,
    args: List[String],
    mainClass: String,
    libs: List[String],
    conf: Map[String, String],
    cluster: String,
    project: String,
    region: String
) extends EtlStep[DPJobEnv, Job] {

  protected def process: RIO[DPJobEnv, Job] = {
    logger.info("#" * 100)
    logger.info(s"Starting Dataproc Spark Job")
    DPJobApi.executeSparkJob(args, mainClass, libs, conf, cluster, project, region)
  }

  override def getStepProperties: Map[String, String] = Map(
    "args"      -> args.mkString(" "),
    "mainClass" -> mainClass,
    "libs"      -> libs.mkString(","),
    "conf"      -> conf.mkString(","),
    "cluster"   -> cluster,
    "project"   -> project,
    "region"    -> region
  )
}
