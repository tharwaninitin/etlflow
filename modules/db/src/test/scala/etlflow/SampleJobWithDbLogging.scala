package etlflow

import etlflow.log.LogEnv
import etlflow.model.Credential.JDBC
import etlflow.task.{DBReadTask, GenericTask}
import zio.blocking.Blocking
import zio.{RIO, ZEnv, ZLayer}

object SampleJobWithDbLogging extends JobApp {

  private val cred = JDBC(sys.env("LOG_DB_URL"), sys.env("LOG_DB_USER"), sys.env("LOG_DB_PWD"), "org.postgresql.Driver")

  override val logLayer: ZLayer[ZEnv, Throwable, LogEnv] = log.DB(cred, java.util.UUID.randomUUID.toString)

  case class EtlJobRun(job_name: String, job_run_id: String, state: String)

  private val task1: DBReadTask[EtlJobRun] = DBReadTask[EtlJobRun](
    name = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,status FROM jobrun LIMIT 10"
  )(rs => EtlJobRun(rs.string("job_name"), rs.string("job_run_id"), rs.string("status")))

  private def processData(ip: List[EtlJobRun]): Unit = {
    logger.info("Processing Data")
    ip.foreach(jr => logger.info(s"$jr"))
  }

  private def task2(ip: List[EtlJobRun]): GenericTask[Unit] = GenericTask(
    name = "ProcessData",
    function = processData(ip)
  )

  def job(args: List[String]): RIO[Blocking with LogEnv, Unit] =
    for {
      op1 <- task1.executeZio.provideSomeLayer[Blocking with LogEnv](db.liveDB(cred))
      _   <- task2(op1).executeZio
    } yield ()
}
