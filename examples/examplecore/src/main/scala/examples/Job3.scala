package examples

import etlflow.audit.AuditEnv
import etlflow.model.Credential.JDBC
import etlflow.task.{DBReadTask, GenericTask}
import etlflow.utils.ApplicationLogger
import zio.Task

object Job3 extends zio.ZIOAppDefault with ApplicationLogger {

  case class EtlJobRun(job_name: String, job_run_id: String, state: String)

  private val cred = JDBC(sys.env("LOG_DB_URL"), sys.env("LOG_DB_USER"), sys.env("LOG_DB_PWD"), "org.postgresql.Driver")

  val task1: DBReadTask[EtlJobRun] = DBReadTask[EtlJobRun](
    name = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun LIMIT 10"
  )(rs => EtlJobRun(rs.string("job_name"), rs.string("job_run_id"), rs.string("state")))

  private def processData(ip: List[EtlJobRun]): Unit = {
    logger.info("Processing Data")
    ip.foreach(jr => logger.info(s"$jr"))
  }

  private def task2(ip: List[EtlJobRun]): GenericTask[Unit] = GenericTask(
    name = "ProcessData",
    function = processData(ip)
  )

  private val job = for {
    op <- task1.execute.provideSomeLayer[AuditEnv](etlflow.db.liveDB(cred))
    _  <- task2(op).execute
  } yield ()

  override def run: Task[Unit] = job.provideLayer(etlflow.audit.noLog)
}
