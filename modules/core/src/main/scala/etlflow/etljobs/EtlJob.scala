package etlflow.etljobs

import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.util.fragment.Fragment
import etlflow.EtlJobProps
import etlflow.jdbc.DbManager
import etlflow.utils.{GlobalProperties, JDBC, JsonJackson, LoggingLevel}
import org.slf4j.{Logger, LoggerFactory}
import zio.interop.catz._
import zio.{Managed, Task, _}

trait EtlJob extends  DbManager{
  final val etl_job_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  var job_name: String = getClass.getName
  val job_properties: EtlJobProps
  val global_properties: Option[GlobalProperties]
  val job_status: UIO[Ref[String]] = Ref.make("StatusNotSet")

  def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit
  def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])]
  def execute(): ZIO[Any, Throwable, Unit]
  def getCredentials[T : Manifest](name: String): T = {

    val credentials: JDBC = JDBC(
      global_properties.get.log_db_url,
      global_properties.get.log_db_user,
      global_properties.get.log_db_pwd,
      global_properties.get.log_db_driver
    )

    lazy val db: Managed[Throwable, HikariTransactor[Task]] =
      createDbTransactorManagedJDBC(credentials, scala.concurrent.ExecutionContext.Implicits.global, name + "-Pool")

    val query = s"SELECT value FROM credentials WHERE name='${name}';"
    etl_job_logger.info("query : " + query)

    val result = db.use { transactor =>
      for {
        result <- Fragment.const(query).query[String].unique.transact(transactor)
      } yield result
    }
    JsonJackson.convertToObject[T](zio.Runtime.default.unsafeRun(result))
  }

}
