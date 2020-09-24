package etlflow.jobs

import etlflow.EtlStepList
import etlflow.Schema.EtlJob4Props
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{EtlStep, GenericETLStep}

case class HelloWorldJob(job_properties: EtlJob4Props) extends SequentialEtlJob[EtlJob4Props] {

  def processData(ip: Unit): Unit = {
    etl_job_logger.info("Hello World")
  }

  val step1 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  override def etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1)
}
