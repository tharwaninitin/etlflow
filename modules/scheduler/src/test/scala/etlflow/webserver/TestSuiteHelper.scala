package etlflow.webserver

import cats.effect.Blocker
import com.zaxxer.hikari.HikariDataSource
import doobie.hikari.HikariTransactor
import etlflow.utils.Executor.DATAPROC
import etlflow.utils.{Config, JDBC, CacheHelper}
import org.testcontainers.containers.PostgreSQLContainer
import zio.Task
import zio.interop.catz._

import scala.concurrent.ExecutionContext
import io.circe.generic.auto._

trait TestSuiteHelper  {

  val cache = CacheHelper.createCache[String](60)
  val canonical_path: String               = new java.io.File(".").getCanonicalPath

  val global_properties: Config = io.circe.config.parser.decode[Config]().toOption.get

  //Creating postgres test container
  val container = new PostgreSQLContainer("postgres:latest")
  container.start()
  val credentials: JDBC = JDBC(container.getJdbcUrl,container.getUsername,container.getPassword,"org.postgresql.Driver")

  def createTestDbTransactorJDBC(credentials:JDBC, ec: ExecutionContext, pool_name: String = "LoggerPool"): HikariTransactor[Task] = {
    val dataSource = new HikariDataSource()
    dataSource.setDriverClassName(credentials.driver)
    dataSource.setJdbcUrl(credentials.url)
    dataSource.setUsername(credentials.user)
    dataSource.setPassword(credentials.password)
    dataSource.setMaximumPoolSize(2)
    dataSource.setPoolName(pool_name)
    HikariTransactor[Task](dataSource, ec, Blocker.liftExecutionContext(ec))
  }

  val transactor = createTestDbTransactorJDBC(credentials,scala.concurrent.ExecutionContext.Implicits.global, "EtlFlowScheduler-testing-Pool")

  val etlJob_name_package: String = "etlflow.scheduler.schema.MyEtlJobName" + "$"

  val dep_libs = sys.env("DP_LIBS")
  val main_class = sys.env("DP_MAIN_CLASS")
  val dp_libs: List[String] = dep_libs.split(",").toList
  val dataproc = DATAPROC(
    sys.env("DP_PROJECT_ID"),
    sys.env("DP_REGION"),
    sys.env("DP_ENDPOINT"),
    sys.env("DP_CLUSTER_NAME")
  )
}
