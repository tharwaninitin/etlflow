package etlflow

import etlflow.audit.Audit
import etlflow.model.Credential.JDBC
import etlflow.task.{DBReadTask, GenericTask}
import zio.{Chunk, RIO, ZLayer}

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
object SampleJobWithDbLogging extends JobApp {

  private val cred = JDBC(sys.env("DB_URL"), sys.env("DB_USER"), sys.env("DB_PWD"), sys.env("DB_DRIVER"))

  override val auditLayer: ZLayer[Any, Throwable, Audit] = audit.DB(cred, java.util.UUID.randomUUID.toString)

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

  def job(args: Chunk[String]): RIO[Audit, Unit] =
    for {
      op1 <- task1.execute.provideSomeLayer[Audit](db.DB.live(cred))
      _   <- task2(op1).execute
    } yield ()
}
