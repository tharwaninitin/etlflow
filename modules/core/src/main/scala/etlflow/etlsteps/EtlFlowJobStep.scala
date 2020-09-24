package etlflow.etlsteps

import etlflow.EtlJobProps
import etlflow.etljobs.EtlJob
import etlflow.utils.LoggingLevel
import zio.{Task, ZEnv}

class EtlFlowJobStep[EJP <: EtlJobProps] private(
        val name: String,
        job: => EtlJob[EJP],
      )
  extends EtlStep[Unit,Unit] {

  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting EtlFlowJobStep for: $name")
    val etl_job = job
    etl_job.job_name = job.toString
    etl_job.execute().provideLayer(ZEnv.live)
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("name" -> name)
}

object EtlFlowJobStep {
  def apply[EJP <: EtlJobProps](name: String, job: => EtlJob[EJP]): EtlFlowJobStep[EJP] =
    new EtlFlowJobStep[EJP](name, job)
}