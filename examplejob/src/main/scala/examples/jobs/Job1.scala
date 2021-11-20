package examples.jobs

import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.GenericETLStep
import examples.schema.MyEtlJobProps.EtlJob1Props

case class Job1(job_properties: EtlJob1Props) extends GenericEtlJob[EtlJob1Props] {

  def processData(ip: Unit): Unit = {
    logger.info(s"Hello World => ${job_properties.arg}")
  }

  val step1 = GenericETLStep(
    name               = "Step_1",
    transform_function = processData,
  )

  val job =
    for {
      _ <- step1.execute(())
    } yield ()

}
