package etlflow

import cats.effect.Blocker
import com.zaxxer.hikari.HikariDataSource
import doobie.hikari.HikariTransactor
import etlflow.Credential.JDBC
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.jdbc.DbManager
import etlflow.utils.EtlFlowHelper.{CronJob, EtlJob}
import etlflow.utils.Executor.DATAPROC
import etlflow.utils.{CacheHelper, Config}
import io.circe.generic.auto._
import scalacache.caffeine.CaffeineCache
import zio.{Queue, Ref, Runtime, Task, ZIO, ZManaged}
import zio.interop.catz._

import scala.concurrent.ExecutionContext
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.utils.EtlFlowUtils
import zio.blocking.Blocking

trait ServerSuiteHelper extends DbManager with EtlFlowUtils {

  val cache: CaffeineCache[String] = CacheHelper.createCache[String]
  val config: Config = io.circe.config.parser.decode[Config]().toOption.get
  val credentials: JDBC = config.dbLog
  type MEJP = MyEtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]]

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
  val transactor: HikariTransactor[Task] = createDbTransactor(credentials,ec,Blocker.liftExecutionContext(ec), "UnManaged-Test-Pool")
  val managedTransactorBlocker: ZManaged[Blocking, Throwable, (HikariTransactor[Task], Blocker)] =
                                for {
                                  blocker     <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
                                  rt          <- Task.runtime.toManaged_
                                  transactor  <- createDbTransactorManaged(config.dbLog, rt.platform.executor.asEC, "Test-Pool")(blocker)
                                } yield (transactor,blocker)
  val etlJob_name_package: String = UF.getJobNamePackage[MyEtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]]] + "$"

  val testJobsQueue = Runtime.default.unsafeRun(Queue.unbounded[(String,String,String,String)])
  val testCronJobs = Runtime.default.unsafeRun(Ref.make(List.empty[CronJob]))
  val testJobsSemaphore = Runtime.default.unsafeRun(createSemaphores(List(EtlJob("Job1",Map("job_max_active_runs" -> "1")))))

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
