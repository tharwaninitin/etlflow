package etlflow.etlsteps

import etlflow.gcp.{DPApi, DPEnv}
import etlflow.model.Executor.DATAPROC
import zio.RIO

case class DPSparkJobStep(name: String, args: List[String], config: DATAPROC, main_class: String, libs: List[String])
    extends EtlStep[DPEnv, Unit] {

  final def process: RIO[DPEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting Dataproc Spark Job")
    DPApi.executeSparkJob(args, main_class, libs, config)
  }

  override def getStepProperties: Map[String, String] = Map(
    "name"       -> name,
    "args"       -> args.mkString(" "),
    "config"     -> config.toString,
    "main_class" -> main_class,
    "libs"       -> libs.mkString(",")
  )
}
