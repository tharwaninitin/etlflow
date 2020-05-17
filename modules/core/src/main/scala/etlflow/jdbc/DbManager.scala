package etlflow.jdbc

import cats.effect.Blocker
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.hikari.HikariTransactor
import etlflow.utils.{GlobalProperties, JDBC}
import zio.interop.catz._
import zio.{Task, ZManaged}
import scala.concurrent.ExecutionContext

trait DbManager {

    def createDbTransactorManaged(global_properties: Option[GlobalProperties], ec: ExecutionContext, pool_name: String = "LoggerPool"): ZManaged[Any, Throwable, HikariTransactor[Task]] = {
      val config = new HikariConfig()
      config.setDriverClassName("org.postgresql.Driver")
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

    def createDbTransactor(global_properties: Option[GlobalProperties], ec: ExecutionContext): HikariTransactor[Task] = {
      val dataSource = new HikariDataSource()
      dataSource.setDriverClassName("org.postgresql.Driver")
      dataSource.setJdbcUrl(global_properties.map(_.log_db_url).getOrElse("<use_global_properties_log_db_url>"))
      dataSource.setUsername(global_properties.map(_.log_db_user).getOrElse("<use_global_properties_log_db_user>"))
      dataSource.setPassword(global_properties.map(_.log_db_pwd).getOrElse("<use_global_properties_log_db_pwd>"))
      dataSource.setMaximumPoolSize(2)
      HikariTransactor[Task](dataSource, ec, Blocker.liftExecutionContext(ec))
    }
  }
