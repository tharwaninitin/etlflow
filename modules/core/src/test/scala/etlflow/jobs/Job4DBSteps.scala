package etlflow.jobs

import etlflow.Schema.{EtlJob4Props, EtlJobRun}
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps._
import etlflow.utils.{Config, JDBC}

case class Job4DBSteps(job_properties: EtlJob4Props) extends GenericEtlJob[EtlJob4Props] {

  private def step1(cred: JDBC) = DBReadStep[EtlJobRun](
    name  = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun LIMIT 10",
    credentials = cred
  )

  private def processData(ip: List[EtlJobRun]): Unit = {
    etl_job_logger.info("Processing Data")
    ip.foreach(jr => etl_job_logger.info(jr.toString))
  }

  private def step2 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  val job =
    for {
      cred  <- getCredentials[JDBC]("etlflow")
      op2   <- step1(cred).execute()
      _     <- step2.execute(op2)
    } yield ()
}
