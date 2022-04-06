package examples

import etlflow.task.{DBReadTask, GenericTask}
import etlflow.log.LogEnv
import etlflow.model.Credential.JDBC
import etlflow.utils.ApplicationLogger
import zio.blocking.Blocking
import zio.{ExitCode, URIO}

object Job3 extends zio.App with ApplicationLogger {

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

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
       op <- task1.executeZio.provideSomeLayer[LogEnv with Blocking](etlflow.db.liveDB(cred))
       _  <- task2(op).executeZio
     } yield ()).provideCustomLayer(etlflow.log.noLog).exitCode
}
