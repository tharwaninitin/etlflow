package etlflow.coretests.jobs

import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.GenericETLStep
import zio.Schedule
import zio.duration._
import etlflow.coretests.Schema.EtlJob2Props

case class Job2Retry(job_properties: EtlJob2Props) extends GenericEtlJob[EtlJob2Props] {

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
