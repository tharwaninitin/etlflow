package etlflow.coretests.jobs

import etlflow.EtlStepList
import etlflow.coretests.Schema.EtlJob1Props
import etlflow.coretests.TestSuiteHelper
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{EtlStep, GenericETLStep}
import etlflow.utils.{ReflectAPI => RF}

case class Job1HelloWorld(job_properties: EtlJob1Props) extends SequentialEtlJob[EtlJob1Props]  with TestSuiteHelper{

  def processData(ip: Unit): Unit = {
    logger.info("Hello World")
  }

  val step1 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  printJobInfo()
  RF.printEtlJobs[MEJP]

  val jobInfo = getJobInfo()

  jobInfo.map(x => (x._1, x._2))
  override def etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1)
}
