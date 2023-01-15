package examples

import etlflow.audit.Audit
import etlflow.log.ApplicationLogger
import etlflow.model.Credential.JDBC
import etlflow.task.{DBReadTask, GenericTask}
import zio.Task

object Job3 extends zio.ZIOAppDefault with ApplicationLogger {

  override val bootstrap = zioSlf4jLogger

  case class EtlJobRun(job_name: String, job_run_id: String, state: String)

  private val cred = JDBC(sys.env("LOG_DB_URL"), sys.env("LOG_DB_USER"), sys.env("LOG_DB_PWD"), "org.postgresql.Driver")

  val task1: DBReadTask[EtlJobRun] = DBReadTask[EtlJobRun](
    name = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun LIMIT 10"
  )(rs => EtlJobRun(rs.string("job_name"), rs.string("job_run_id"), rs.string("state")))

  private def processData(ip: Iterable[EtlJobRun]): Unit = {
    logger.info("Processing Data")
    ip.foreach(jr => logger.info(s"$jr"))
  }

  private def task2(ip: Iterable[EtlJobRun]): GenericTask[Unit] = GenericTask(
    name = "ProcessData",
    function = processData(ip)
  )

  private val job = for {
    op <- task1.execute.provideSomeLayer[Audit](etlflow.db.DB.live(cred))
    _  <- task2(op).execute
  } yield ()

  override def run: Task[Unit] = job.provideLayer(etlflow.audit.noop)
}
