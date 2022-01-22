package etlflow.jobtests.jobs

import etlflow.jobtests.MyEtlJobProps.EtlJob5Props
import etlflow.etljobs.EtlJob
import etlflow.etlsteps._

case class Job4GenericSteps(job_properties: EtlJob5Props) extends EtlJob[EtlJob5Props] {

  private def processData(): Unit = {
    logger.info("Processing Data")
    throw new RuntimeException("Exception in Step")
  }

  val step1 = GenericETLStep(
    name = "ProcessData",
    function = processData()
  )

  val job = step1.execute
}
