package examples

import etlflow.JobApp
import etlflow.etlsteps.{DBReadStep, GenericETLStep}
import etlflow.log.LogEnv
import etlflow.model.Credential.JDBC
import zio.ZLayer

object Job4 extends JobApp {

  case class EtlJobRun(job_name: String, job_run_id: String, state: String)

  val cred = JDBC(sys.env("LOG_DB_URL"), sys.env("LOG_DB_USER"), sys.env("LOG_DB_PWD"), sys.env("LOG_DB_DRIVER"))

  override val log_layer: ZLayer[zio.ZEnv, Throwable, LogEnv] = etlflow.log.DB(cred, java.util.UUID.randomUUID.toString)

  private def step1(cred: JDBC): DBReadStep[EtlJobRun] = DBReadStep[EtlJobRun](
    name = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,status FROM jobrun LIMIT 10",
    credentials = cred
  )(rs => EtlJobRun(rs.string("job_name"), rs.string("job_run_id"), rs.string("status")))

  private def processData(ip: List[EtlJobRun]): Unit = {
    logger.info("Processing Data")
    ip.foreach(jr => logger.info(jr.toString))
  }

  private def step2(ip: List[EtlJobRun]): GenericETLStep[Unit] = GenericETLStep(
    name = "ProcessData",
    function = processData(ip)
  )

  def job(args: List[String]) =
    for {
      op <- step1(cred).execute
      _  <- step2(op).execute
    } yield ()
}
