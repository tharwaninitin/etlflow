package etlflow.jdbc

import cats.effect.Blocker
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import etlflow.utils.{GlobalProperties, JDBC}
import io.getquill.{LowerCase, PostgresJdbcContext}
import org.flywaydb.core.Flyway
import org.slf4j.{Logger, LoggerFactory}
import zio.interop.catz._
import zio.{Task, ZManaged}
import scala.concurrent.ExecutionContext

trait DbManager {

  def createDbTransactorManagedGP(global_properties: Option[GlobalProperties], ec: ExecutionContext, pool_name: String = "LoggerPool"): ZManaged[Any, Throwable, HikariTransactor[Task]] = {
    val config = new HikariConfig()
    config.setDriverClassName(global_properties.map(_.log_db_driver).getOrElse("<use_global_properties_log_db_driver>"))
    config.setJdbcUrl(global_properties.map(_.log_db_url).getOrElse("<use_global_properties_log_db_url>"))
    config.setUsername(global_properties.map(_.log_db_user).getOrElse("<use_global_properties_log_db_user>"))
    config.setPassword(global_properties.map(_.log_db_pwd).getOrElse("<use_global_properties_log_db_pwd>"))
    config.setMaximumPoolSize(2)
    config.setPoolName(pool_name)
    HikariTransactor.fromHikariConfig[Task](config, ec, Blocker.liftExecutionContext(ec))
  }.toManagedZIO

  def createDbTransactorManagedJDBC(credentials: JDBC, ec: ExecutionContext, pool_name: String = "LoggerPool", pool_size: Int = 2): ZManaged[Any, Throwable, HikariTransactor[Task]] = {
    val config = new HikariConfig()
    config.setDriverClassName(credentials.driver)
    config.setJdbcUrl(credentials.url)
    config.setUsername(credentials.user)
    config.setPassword(credentials.password)
    config.setMaximumPoolSize(pool_size)
    config.setPoolName(pool_name)
    HikariTransactor.fromHikariConfig[Task](config, ec, Blocker.liftExecutionContext(ec))
    }.toManagedZIO

  def createDbTransactorManagedJDBC(credentials: JDBC): ZManaged[Any, Throwable, HikariTransactor[Task]] = {
    (for {
      connectEC <- ExecutionContexts.fixedThreadPool[Task](10)
      xa        <- HikariTransactor.newHikariTransactor[Task](
        credentials.driver,     // driver classname
        credentials.url,        // connect URL
        credentials.user,       // username
        credentials.password,   // password
        connectEC,                              // await connection here
        Blocker.liftExecutionContext(connectEC) // transactEC // execute JDBC operations here
      )
    } yield xa).toManagedZIO
  }

  def runDbMigration(credentials: JDBC): Task[Int] = Task {
    val logger: Logger = LoggerFactory.getLogger(getClass.getName)
    val configuration = Flyway
      .configure(this.getClass.getClassLoader)
      .dataSource(credentials.url, credentials.user, credentials.password)
      .locations("migration")
      .connectRetries(10)
      .load()
    logger.info("Running db migration from paths:")
    logger.info(configuration.info().all().toList.map(x => x.getPhysicalLocation).mkString("\n","\n",""))
    configuration.migrate()
  }

  def createDbTransactor(global_properties: Option[GlobalProperties], ec: ExecutionContext, pool_name: String = "LoggerPool"): HikariTransactor[Task] = {
    val dataSource = new HikariDataSource()
    dataSource.setDriverClassName(global_properties.map(_.log_db_driver).getOrElse("<use_global_properties_log_db_driver>"))
    dataSource.setJdbcUrl(global_properties.map(_.log_db_url).getOrElse("<use_global_properties_log_db_url>"))
    dataSource.setUsername(global_properties.map(_.log_db_user).getOrElse("<use_global_properties_log_db_user>"))
    dataSource.setPassword(global_properties.map(_.log_db_pwd).getOrElse("<use_global_properties_log_db_pwd>"))
    dataSource.setMaximumPoolSize(2)
    dataSource.setPoolName(pool_name)
    HikariTransactor[Task](dataSource, ec, Blocker.liftExecutionContext(ec))
  }

  def createDbTransactorJDBC(credentials: JDBC, ec: ExecutionContext, blocker: Blocker, pool_name: String = "LoggerPool", pool_size: Int = 2): Task[HikariTransactor[Task]] = Task {
    val dataSource = new HikariDataSource()
    dataSource.setDriverClassName(credentials.driver)
    dataSource.setJdbcUrl(credentials.url)
    dataSource.setUsername(credentials.user)
    dataSource.setPassword(credentials.password)
    dataSource.setMaximumPoolSize(pool_size)
    dataSource.setPoolName(pool_name)
    HikariTransactor[Task](dataSource, ec, blocker)
  }

  def createDbContextJDBC(credentials: JDBC, pool_name: String = "LoggerPool", pool_size: Int = 2): Task[PostgresJdbcContext[LowerCase.type]] = Task {
    val pgDataSource = new org.postgresql.ds.PGSimpleDataSource()
    pgDataSource.setURL(credentials.url)
    pgDataSource.setUser(credentials.user)
    pgDataSource.setPassword(credentials.password)
    val config = new HikariConfig()
    config.setDataSource(pgDataSource)
    config.setMaximumPoolSize(pool_size)
    config.setPoolName(pool_name)
    new PostgresJdbcContext(LowerCase, new HikariDataSource(config))
  }
}
