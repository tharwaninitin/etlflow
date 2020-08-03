package etlflow.etlsteps

import etlflow.EtlJobProps
import etlflow.etljobs.EtlJob
import etlflow.utils.{GlobalProperties, LoggingLevel}
import zio.Task

import scala.reflect.runtime.universe.TypeTag

case class EtlFlowJobStep[EJP <: EtlJobProps : TypeTag, EJGP <: GlobalProperties : TypeTag](
                                                                                             name: String,
                                                                                             job: (EJP,Option[EJGP]) => EtlJob,
                                                                                             props: EJP,
                                                                                             conf: Option[EJGP]
                                                                                           )
  extends EtlStep[Unit,Unit] {

  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting EtlFlowJobStep for: $name")
    val etl_job = job(props,conf)
    etl_job.job_name = job.toString
    etl_job.execute()
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("name" -> name)
}
