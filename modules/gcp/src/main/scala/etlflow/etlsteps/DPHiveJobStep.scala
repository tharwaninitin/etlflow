package etlflow.etlsteps

import etlflow.gcp.{DP, DPApi}
import etlflow.schema.Executor.DATAPROC
import etlflow.schema.LoggingLevel
import zio.Task

case class DPHiveJobStep(
                          name: String,
                          query: String,
                          config: DATAPROC,
                        )
  extends EtlStep[Unit,Unit] {

  final def process(in: =>Unit): Task[Unit] = {
    val env = DP.live(config)
    logger.info("#"*100)
    logger.info(s"Starting Hive Dataproc Job: $name")
    DPApi.executeHiveJob(query).provideLayer(env)
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> query)
}


