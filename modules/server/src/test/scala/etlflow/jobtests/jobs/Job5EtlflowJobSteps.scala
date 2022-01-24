package etlflow.jobtests.jobs

import etlflow.jobtests.MyEtlJobProps.EtlJob1Props
import etlflow.etljobs.EtlJob
import etlflow.etlsteps.EtlFlowJobStep
import etlflow.log
import zio.{RIO, ZEnv}

case class Job5EtlflowJobSteps(job_properties: EtlJob1Props) extends EtlJob[EtlJob1Props] {

  val step1: EtlFlowJobStep[EtlJob1Props] = EtlFlowJobStep[EtlJob1Props](
    name = "Test",
    job = Job1HelloWorld(EtlJob1Props())
  )

  override def job: RIO[ZEnv with log.LogEnv, Unit] = step1.execute
}
