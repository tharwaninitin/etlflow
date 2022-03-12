package examples

import etlflow.etlsteps.{DBReadStep, GenericETLStep}
import etlflow.log.LogEnv
import etlflow.model.Credential.JDBC
import etlflow.utils.ApplicationLogger
import zio.blocking.Blocking
import zio.{ExitCode, URIO}

object Job3 extends zio.App with ApplicationLogger {

  case class EtlJobRun(job_name: String, job_run_id: String, state: String)

  private val cred = JDBC(sys.env("LOG_DB_URL"), sys.env("LOG_DB_USER"), sys.env("LOG_DB_PWD"), "org.postgresql.Driver")

  val step1: DBReadStep[EtlJobRun] = DBReadStep[EtlJobRun](
    name = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun LIMIT 10"
  )(rs => EtlJobRun(rs.string("job_name"), rs.string("job_run_id"), rs.string("state")))

  private def processData(ip: List[EtlJobRun]): Unit = {
    logger.info("Processing Data")
    ip.foreach(jr => logger.info(s"$jr"))
  }

  private def step2(ip: List[EtlJobRun]): GenericETLStep[Unit] = GenericETLStep(
    name = "ProcessData",
    function = processData(ip)
  )

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
       op <- step1.execute.provideSomeLayer[LogEnv with Blocking](etlflow.db.liveDB(cred))
       _  <- step2(op).execute
     } yield ()).provideCustomLayer(etlflow.log.noLog).exitCode
}
