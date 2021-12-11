package examples.jobs

import etlflow.etljobs.EtlJob
import etlflow.etlsteps.GenericETLStep
import examples.schema.MyEtlJobProps.LocalSampleProps

case class EtlJobGenericStep(job_properties: LocalSampleProps) extends EtlJob[LocalSampleProps] {

  def processData(ip: Unit): Unit = {
    logger.info("Hello World")
    throw new RuntimeException("Error123")
  }

  val step1 = {
    GenericETLStep(
      name               = "Step_1",
      transform_function = processData,
    )
  }

  val step2 = {
    GenericETLStep(
      name               = "Step_2",
      transform_function = processData,
    )
  }

  val step3 = {
    GenericETLStep(
      name               = "Step_3",
      transform_function = processData,
    )
  }

  override val job = for {
    -  <- step1.execute(())
    -  <- step2.execute(())
    -  <- step3.execute(())
  } yield ()
}
