package etlflow.jdbc

import cats.effect.{Blocker, Resource}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import etlflow.utils.{GlobalProperties, JDBC}
import org.flywaydb.core.Flyway
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

  def createDbTransactorManagedJDBC(credentials: JDBC, ec: ExecutionContext, pool_name: String = "LoggerPool"): ZManaged[Any, Throwable, HikariTransactor[Task]] = {
    val config = new HikariConfig()
    config.setDriverClassName(credentials.driver)
    config.setJdbcUrl(credentials.url)
    config.setUsername(credentials.user)
    config.setPassword(credentials.password)
    config.setMaximumPoolSize(2)
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
    Flyway
      .configure(this.getClass.getClassLoader)
      .dataSource(credentials.url, credentials.user, credentials.password)
      .locations("migration")
      .connectRetries(2)
      .load()
      .migrate()
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

  def createDbTransactorJDBC(credentials: JDBC, ec: ExecutionContext, pool_name: String = "LoggerPool"): Task[HikariTransactor[Task]] = Task {
    val dataSource = new HikariDataSource()
    dataSource.setDriverClassName(credentials.driver)
    dataSource.setJdbcUrl(credentials.url)
    dataSource.setUsername(credentials.user)
    dataSource.setPassword(credentials.password)
    dataSource.setMaximumPoolSize(2)
    dataSource.setPoolName(pool_name)
    HikariTransactor[Task](dataSource, ec, Blocker.liftExecutionContext(ec))
  }
}
