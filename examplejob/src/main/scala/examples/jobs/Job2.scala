package examples.jobs

import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.GenericETLStep
import examples.schema.MyEtlJobProps.EtlJob1Props

case class Job2(job_properties: EtlJob1Props) extends GenericEtlJob[EtlJob1Props] {

  def processData1(ip: Unit): String = {
    logger.info(s"Hello World => ${job_properties.arg}")
    job_properties.arg
  }

  val step1 = GenericETLStep(
    name               = "Step_1",
    transform_function = processData1,
  )

  def processData2(ip: String): Unit = {
    logger.info(s"Hello World => $ip")
  }

  val step2 = GenericETLStep(
    name               = "Step_2",
    transform_function = processData2,
  )

  def processData3(ip: Unit): Unit = {
    logger.info(s"Hello World => ${job_properties.arg}")
    throw new RuntimeException("Error123")
  }

  val step3 = GenericETLStep(
    name               = "Step_3",
    transform_function = processData3,
  )

  val job =
    for {
      op <- step1.execute(())
      _  <- step2.execute(op)
      _  <- step3.execute(())
    } yield ()

}
