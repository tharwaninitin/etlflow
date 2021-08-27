package examples.jobs

import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{EtlStep, GenericETLStep}
import examples.schema.MyEtlJobProps.LocalSampleProps

case class EtlJob8DefinitionLocal(job_properties: LocalSampleProps) extends SequentialEtlJob[LocalSampleProps] {

  def processData(ip: Unit): Unit = {
    logger.info("Hello World")
  }

  val step1 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  printJobInfo()

  val jobInfo = getJobInfo()

  jobInfo.map(x => (x._1, x._2))
  override def etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1)
}
