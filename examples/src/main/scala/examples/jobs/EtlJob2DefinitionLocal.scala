package examples.jobs

import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.GenericETLStep
import examples.schema.MyEtlJobProps.LocalSampleProps
import zio.Schedule
import zio.duration._

case class EtlJob2DefinitionLocal(job_properties: LocalSampleProps) extends GenericEtlJob[LocalSampleProps] {

  def processData(ip: Unit): Unit = {
    etl_job_logger.info("Hello World")
    Thread.sleep(10000)
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
    -  <- step1.execute().retry(Schedule.spaced(1.second) && Schedule.recurs(2))
    -  <- step2.execute().retry(Schedule.spaced(1.second) && Schedule.recurs(2))
    -  <- step3.execute().retry(Schedule.spaced(1.second) && Schedule.recurs(2))

  } yield ()
}
