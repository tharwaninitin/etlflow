package examples.jobs

import etlflow.etljobs.EtlJob
import etlflow.etlsteps.GenericETLStep
import examples.schema.MyEtlJobProps.LocalSampleProps

case class EtlJobGenericStep(job_properties: LocalSampleProps) extends EtlJob[LocalSampleProps] {

  def processData(): Unit = {
    logger.info("Hello World")
    throw new RuntimeException("Error123")
  }

  val step1 = GenericETLStep(
    name = "Step_1",
    function = processData
  )

  val step2 = GenericETLStep(
    name = "Step_2",
    function = processData
  )

  val step3 = GenericETLStep(
    name = "Step_3",
    function = processData
  )

  override val job = for {
    _ <- step1.execute
    _ <- step2.execute
    _ <- step3.execute
  } yield ()
}
