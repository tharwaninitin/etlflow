package etlflow.coretests.jobs

import etlflow.EtlStepList
import etlflow.coretests.Schema.EtlJob1Props
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{EtlStep, GenericETLStep}

case class Job1HelloWorld(job_properties: EtlJob1Props) extends SequentialEtlJob[EtlJob1Props] {

  def processData(ip: Unit): Unit = {
    etl_job_logger.info("Hello World")
  }

  val step1 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  override def etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1)
}
