package etlflow.task

import com.google.cloud.dataproc.v1.Job
import gcp4zio.dp._
import zio.{Config, RIO}

case class DPSparkJobTask(
    name: String,
    args: List[String],
    mainClass: String,
    libs: List[String],
    conf: Map[String, String],
    cluster: String,
    project: String,
    region: String
) extends EtlTask[DPJob, Job] {

  override protected def process: RIO[DPJob, Job] = {
    logger.info("#" * 100)
    logger.info(s"Starting Dataproc Spark Job")
    DPJob.executeSparkJob(args, mainClass, libs, conf, cluster, project, region)
  }

  override def getTaskProperties: Map[String, String] = Map(
    "args"      -> args.mkString(" "),
    "mainClass" -> mainClass,
    "libs"      -> libs.mkString(","),
    "conf"      -> conf.mkString(","),
    "cluster"   -> cluster,
    "project"   -> project,
    "region"    -> region
  )
}

object DPSparkJobTask {
  val config: Config[DPSparkJobTask] = Config
    .string("name")
    .zip(Config.listOf("args", Config.string))
    .zip(Config.string("mainClass"))
    .zip(Config.listOf("libs", Config.string))
    // .zip(Config.table("conf", Config.string))
    .zip(Config.string("cluster"))
    .zip(Config.string("project"))
    .zip(Config.string("region"))
    .map { case (host, args, mainClass, libs, cluster, project, region) =>
      DPSparkJobTask(host, args, mainClass, libs, Map.empty, cluster, project, region)
    }
}
