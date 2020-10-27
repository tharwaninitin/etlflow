package etlflow.etlsteps

import etlflow.EtlJobProps
import etlflow.etljobs.EtlJob
import etlflow.utils.LoggingLevel
import zio.{Task, ZEnv}
import etlflow.utils.JsonJackson
class EtlFlowJobStep[EJP <: EtlJobProps] private(
                                                  val name: String,
                                                  job: => EtlJob[EJP],
                                                )
  extends EtlStep[Unit,Unit] {

  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting EtlFlowJobStep for: $name")
    job.execute().provideLayer(ZEnv.live)
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] =  {
    val excludeKeys = List("job_run_id","job_description","job_properties","job_aggregate_error")
    JsonJackson.convertToJsonByRemovingKeysAsMap(job.job_properties,excludeKeys).mapValues( _.toString)
  }
}

object EtlFlowJobStep {
  def apply[EJP <: EtlJobProps](name: String, job: => EtlJob[EJP]): EtlFlowJobStep[EJP] =
    new EtlFlowJobStep[EJP](name, job)
}