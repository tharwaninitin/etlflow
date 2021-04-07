package etlflow

import cats.effect.Blocker
import com.zaxxer.hikari.HikariDataSource
import doobie.hikari.HikariTransactor
import etlflow.Credential.JDBC
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.etljobs.EtlJob
import etlflow.jdbc.DbManager
import etlflow.utils.Executor.DATAPROC
import etlflow.utils.{CacheHelper, Config}
import io.circe.generic.auto._
import scalacache.Cache
import zio.{Queue, Runtime, Task}
import zio.interop.catz._
import scala.concurrent.ExecutionContext

trait SchedulerSuiteHelper extends DbManager {

  val cache: Cache[String] = CacheHelper.createCache[String]
  val global_properties: Config = io.circe.config.parser.decode[Config]().toOption.get
  val credentials: JDBC = global_properties.dbLog

  def createDbTransactor(credentials: JDBC, ec: ExecutionContext, blocker: Blocker, pool_name: String = "LoggerPool", pool_size: Int = 2): HikariTransactor[Task] = {
    val dataSource = new HikariDataSource()
    dataSource.setDriverClassName(credentials.driver)
    dataSource.setJdbcUrl(credentials.url)
    dataSource.setUsername(credentials.user)
    dataSource.setPassword(credentials.password)
    dataSource.setMaximumPoolSize(pool_size)
    dataSource.setPoolName(pool_name)
    HikariTransactor[Task](dataSource, ec, blocker)
  }

  val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
  val transactor: HikariTransactor[Task] = createDbTransactor(credentials,ec,Blocker.liftExecutionContext(ec), "EtlFlow-Scheduler-Testing-Pool")
  val managedTransactor = createDbTransactorManaged(credentials,ec, "EtlFlow-Managed-Scheduler-Testing-Pool")
  val etlJob_name_package: String = UF.getJobNamePackage[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]] + "$"

  val jobTestQueue = Runtime.default.unsafeRun(Queue.unbounded[(String,String,String,String)])
  lazy val dp_dep_libs: String = sys.env("DP_LIBS")
  lazy val dp_main_class: String = sys.env("DP_MAIN_CLASS")
  lazy val dp_libs: List[String] = dp_dep_libs.split(",").toList
  lazy val dataproc: DATAPROC = DATAPROC(
    sys.env("DP_PROJECT_ID"),
    sys.env("DP_REGION"),
    sys.env("DP_ENDPOINT"),
    sys.env("DP_CLUSTER_NAME")
  )
}
