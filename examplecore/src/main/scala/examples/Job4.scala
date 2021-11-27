package examples

import etlflow.etlsteps.{DBReadStep, GenericETLStep}
import etlflow.schema.Credential.JDBC
import etlflow.utils.ApplicationLogger
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{ExitCode, URIO}

object Job4 extends zio.App with ApplicationLogger {

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

  val job =
    for {
      op1 <- step1(cred).execute(())
      _ <- step2.execute(op1)
    } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val layer = etlflow.log.DBLiveImplementation(cred) ++ etlflow.log.Implementation.live ++ etlflow.log.ConsoleImplementation.live ++ etlflow.log.SlackImplementation.nolog
    job.provideSomeLayer[Blocking with Clock](layer).exitCode
  }
}
