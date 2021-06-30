package etlflow.coretests.jobs

import etlflow.coretests.Schema.EtlJob1Props
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.EtlFlowJobStep
import io.circe.generic.auto._

case class Job5EtlflowJobSteps(job_properties: EtlJob1Props) extends GenericEtlJob[EtlJob1Props] {

  def processData(ip: Unit): Unit = {
    logger.info("Hello World")
  }

  val step1: EtlFlowJobStep[EtlJob1Props] = EtlFlowJobStep[EtlJob1Props](
    name = "Test",
    job  = Job1HelloWorld(EtlJob1Props())
  )

  override val job = for {
    -       <- step1.execute()
  } yield ()
}
