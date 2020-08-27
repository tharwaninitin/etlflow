package etlflow.jobs

import etlflow.{EtlJobProps, EtlStepList}
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{EtlStep, GenericETLStep}
import etlflow.utils.Config

case class HelloWorldJob(job_properties: EtlJobProps, globalProperties: Config) extends SequentialEtlJob {

  def processData(ip: Unit): Unit = {
    etl_job_logger.info("Hello World")
  }

  val step1 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  override def etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1)
}
