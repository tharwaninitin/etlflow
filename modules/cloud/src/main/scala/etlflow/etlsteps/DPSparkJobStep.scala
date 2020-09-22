package etlflow.etlsteps

import etlflow.gcp.{DP, DPService}
import etlflow.utils.Executor.DATAPROC
import etlflow.utils.LoggingLevel
import zio.Task

case class DPSparkJobStep(
                           name: String,
                           job_name: String,
                           props: Map[String,String],
                           config: DATAPROC,
                           main_class: String,
                           libs: List[String]
                         )
  extends EtlStep[Unit,Unit] {

  final def process(in: =>Unit): Task[Unit] = {
    val env = DP.live(config)
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting Job Submission for: $job_name")
    DPService.executeSparkJob(job_name,props,main_class,libs).provideLayer(env)
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> job_name)
}


