package etlflow.scheduler

import cats.effect.Blocker
import com.zaxxer.hikari.HikariDataSource
import doobie.hikari.HikariTransactor
import etlflow.scheduler.util.CacheHelper
import etlflow.utils.{Config, JDBC}
import org.testcontainers.containers.PostgreSQLContainer
import zio.Task
import zio.interop.catz._
import scala.concurrent.ExecutionContext
import io.circe.generic.auto._

trait TestSuiteHelper  {

  val cache = CacheHelper.createCache[String](60)
  val canonical_path: String               = new java.io.File(".").getCanonicalPath
//  val global_properties: Option[GlobalProperties] =
//    Try(new GlobalProperties(canonical_path + "/modules/scheduler/src/test/resources/loaddata.properties") {}).toOption

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

}
