package examples.jobs

import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.GenericETLStep
import etlflow.utils.Config
import examples.schema.MyEtlJobProps
import zio.Schedule
import zio.duration._

case class EtlJob2DefinitionLocal(job_properties: MyEtlJobProps, globalProperties: Config) extends GenericEtlJob {

  //throw new RuntimeException("!! Error in job instance creation")

  def processData(ip: Unit): Unit = {
    etl_job_logger.info("Hello World")
    Thread.sleep(10000)
    //throw new RuntimeException("!! step failed")
  }

  val step1 = {
    GenericETLStep(
      name               = "ProcessData",
      transform_function = processData,
    )
  }

  override val job = for {
    -       <- step1.execute().retry(Schedule.spaced(1.second) && Schedule.recurs(2))
  } yield ()
}
