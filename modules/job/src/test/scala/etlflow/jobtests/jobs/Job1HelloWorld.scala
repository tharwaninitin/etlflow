package etlflow.jobtests.jobs

import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{EtlStep, GenericETLStep}
import etlflow.jobtests.MyEtlJobProps.EtlJob1Props

case class Job1HelloWorld(job_properties: EtlJob1Props) extends SequentialEtlJob[EtlJob1Props] {

  def processData(ip: Unit): Unit = {
    logger.info("Hello World")
  }

  val step1 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  override def etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1)
}
