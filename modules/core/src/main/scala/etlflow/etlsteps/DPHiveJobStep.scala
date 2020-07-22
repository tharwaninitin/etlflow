package etlflow.etlsteps

import etlflow.gcp.{DP, DPService}
import etlflow.utils.Executor.DATAPROC
import zio.Task

case class DPHiveJobStep(
                          name: String,
                          query: String,
                          config: DATAPROC,
     )
  extends EtlStep[Unit,Unit] {

  final def process(in: =>Unit): Task[Unit] = {
    val env = DP.live(config)
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting Hive Dataproc Job: $name")
    DPService.executeHiveJob(query).provideLayer(env)
  }

  override def getStepProperties(level: String): Map[String, String] = Map("query" -> query)
}


