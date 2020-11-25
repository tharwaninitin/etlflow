package etlflow.jobs

import etlflow.Schema.EtlJob5Props
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps._

case class Job5GenricSteps(job_properties: EtlJob5Props) extends GenericEtlJob[EtlJob5Props] {

  private def processData(ip: String): Unit = {
    etl_job_logger.info("Processing Data")
    throw new RuntimeException("Invalid")
    ip.foreach(jr => etl_job_logger.info(jr.toString))
  }

  private def step1 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  val job =
    for {
      _     <- step1.execute("Sample")
    } yield ()
}
