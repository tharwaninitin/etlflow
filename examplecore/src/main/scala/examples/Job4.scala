package examples

import etlflow.EtlFlowApp
import etlflow.core.StepEnv
import etlflow.etlsteps.{DBReadStep, GenericETLStep}
import etlflow.schema.Credential.JDBC
import zio.RIO

object Job4 extends EtlFlowApp {

  case class EtlJobRun(job_name: String, job_run_id: String, state: String)

  val cred = JDBC(sys.env("LOG_DB_URL"), sys.env("LOG_DB_USER"), sys.env("LOG_DB_PWD"), "org.postgresql.Driver")

  private def step1(cred: JDBC): DBReadStep[EtlJobRun] = DBReadStep[EtlJobRun](
    name = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun LIMIT 10",
    credentials = cred
  )(rs => EtlJobRun(rs.string("job_name"), rs.string("job_run_id"), rs.string("state")))

  private def processData(ip: List[EtlJobRun]): Unit = {
    logger.info("Processing Data")
    ip.foreach(jr => logger.info(jr.toString))
  }

  private def step2: GenericETLStep[List[EtlJobRun], Unit] = GenericETLStep(
    name = "ProcessData",
    transform_function = processData,
  )

  def job(args: List[String]): RIO[StepEnv, Unit] =
    for {
      op1 <- step1(cred).execute(())
      _ <- step2.execute(op1)
    } yield ()
}
