package etlflow.jobtests.jobs

import etlflow.etljobs.EtlJob
import etlflow.etlsteps.GenericETLStep
import etlflow.jobtests.MyEtlJobProps.EtlJob1Props

case class Job6HelloWorld(job_properties: EtlJob1Props) extends EtlJob[EtlJob1Props] {

  def processData(): Unit = logger.info(s"Hello World")

  val step1 = GenericETLStep(
    name = "ProcessData",
    function = processData()
  )

  override val job = step1.execute
}
