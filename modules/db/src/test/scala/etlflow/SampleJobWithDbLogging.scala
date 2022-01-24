package etlflow

import etlflow.etlsteps.{DBReadStep, GenericETLStep}
import etlflow.log.LogEnv
import etlflow.model.Credential.JDBC
import zio.blocking.Blocking
import zio.{RIO, ZEnv, ZLayer}

object SampleJobWithDbLogging extends JobApp {

  val cred = JDBC(sys.env("LOG_DB_URL"), sys.env("LOG_DB_USER"), sys.env("LOG_DB_PWD"), "org.postgresql.Driver")

  override val log_layer: ZLayer[ZEnv, Throwable, LogEnv] = log.DB(cred, java.util.UUID.randomUUID.toString)

  case class EtlJobRun(job_name: String, job_run_id: String, state: String)

  private val step1: DBReadStep[EtlJobRun] = DBReadStep[EtlJobRun](
    name = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,status FROM jobrun LIMIT 10"
  )(rs => EtlJobRun(rs.string("job_name"), rs.string("job_run_id"), rs.string("status")))

  private def processData(ip: List[EtlJobRun]): Unit = {
    logger.info("Processing Data")
    ip.foreach(jr => logger.info(jr.toString))
  }

  private def step2(ip: List[EtlJobRun]): GenericETLStep[Unit] = GenericETLStep(
    name = "ProcessData",
    function = processData(ip)
  )

  def job(args: List[String]): RIO[Blocking with LogEnv, Unit] =
    for {
      op1 <- step1.execute.provideSomeLayer[Blocking with LogEnv](db.liveDB(cred))
      _   <- step2(op1).execute
    } yield ()
}