package etlflow.jdbc

import cats.effect.Blocker
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.hikari.HikariTransactor
import etlflow.utils.JDBC
import org.flywaydb.core.Flyway
import org.slf4j.{Logger, LoggerFactory}
import zio.interop.catz._
import zio.{Task, ZManaged}
import scala.concurrent.ExecutionContext

trait DbManager {

  def createDbTransactorManaged(
        credentials: JDBC,
        ec: ExecutionContext,
        pool_name: String = "LoggerPool",
        pool_size: Int = 2
       )
       (implicit blocker: Blocker = Blocker.liftExecutionContext(ec))
  : ZManaged[Any, Throwable, HikariTransactor[Task]] = {
    val config = new HikariConfig()
    config.setDriverClassName(credentials.driver)
    config.setJdbcUrl(credentials.url)
    config.setUsername(credentials.user)
    config.setPassword(credentials.password)
    config.setMaximumPoolSize(pool_size)
    config.setPoolName(pool_name)
    HikariTransactor.fromHikariConfig[Task](config, ec, blocker)
  }.toManagedZIO

  def runDbMigration(credentials: JDBC, clean: Boolean = false): Task[Int] = Task {
    val logger: Logger = LoggerFactory.getLogger(getClass.getName)
    val configuration = Flyway
      .configure(this.getClass.getClassLoader)
      .dataSource(credentials.url, credentials.user, credentials.password)
      .locations("migration")
      .connectRetries(10)
      .load()
    logger.info("Running db migration from paths:")
    logger.info(configuration.info().all().toList.map(x => x.getPhysicalLocation).mkString("\n","\n",""))
    if (clean) configuration.clean()
    configuration.migrate()
  }

  def createDbTransactor(credentials: JDBC, ec: ExecutionContext, blocker: Blocker, pool_name: String = "LoggerPool", pool_size: Int = 2): Task[HikariTransactor[Task]] = Task {
    val dataSource = new HikariDataSource()
    dataSource.setDriverClassName(credentials.driver)
    dataSource.setJdbcUrl(credentials.url)
    dataSource.setUsername(credentials.user)
    dataSource.setPassword(credentials.password)
    dataSource.setMaximumPoolSize(pool_size)
    dataSource.setPoolName(pool_name)
    HikariTransactor[Task](dataSource, ec, blocker)
  }

}
