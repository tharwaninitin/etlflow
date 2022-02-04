package etlflow.etlsteps

import gcp4zio._
import zio.RIO

case class DPSparkJobStep(
    name: String,
    args: List[String],
    main_class: String,
    libs: List[String],
    conf: Map[String, String],
    clusterName: String,
    project: String,
    region: String
) extends EtlStep[DPJobEnv, Unit] {

  final def process: RIO[DPJobEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting Dataproc Spark Job")
    DPJobApi.executeSparkJob(args, main_class, libs, conf, clusterName: String, project: String, region: String)
  }

  override def getStepProperties: Map[String, String] = Map(
    "args"        -> args.mkString(" "),
    "main_class"  -> main_class,
    "libs"        -> libs.mkString(","),
    "conf"        -> conf.mkString(","),
    "clusterName" -> clusterName,
    "project"     -> project,
    "region"      -> region
  )
}
