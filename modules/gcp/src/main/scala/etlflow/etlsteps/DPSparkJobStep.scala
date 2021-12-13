package etlflow.etlsteps

import etlflow.gcp.{DP, DPApi}
import etlflow.schema.Executor.DATAPROC
import zio.Task

case class DPSparkJobStep(
                           name: String,
                           args: List[String],
                           config: DATAPROC,
                           main_class: String,
                           libs: List[String]
                         )
  extends EtlStep[Unit,Unit] {

  final def process(in: =>Unit): Task[Unit] = {
    val env = DP.live(config)
    logger.info("#" * 100)
    logger.info(s"Starting Dataproc Spark Job")
    DPApi.executeSparkJob(args,main_class,libs).provideLayer(env)
  }

  override def getStepProperties: Map[String, String] =
    Map(
      "name" -> name,
      "args" -> args.mkString(" "),
      "config" -> config.toString,
      "main_class" -> main_class,
      "libs" -> libs.mkString(",")
    )
}