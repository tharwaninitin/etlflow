package etlflow.jobtests.jobs

import etlflow.core.CoreLogEnv
import etlflow.jobtests.MyEtlJobProps.EtlJob1Props
import etlflow.etljobs.EtlJob
import etlflow.etlsteps.EtlFlowJobStep
import zio.RIO

case class Job5EtlflowJobSteps(job_properties: EtlJob1Props) extends EtlJob[EtlJob1Props] {

  val step1: EtlFlowJobStep[EtlJob1Props] = EtlFlowJobStep[EtlJob1Props](
    name = "Test",
    job = Job1HelloWorld(EtlJob1Props())
  )

  override def job: RIO[CoreLogEnv, Unit] = step1.execute
}
