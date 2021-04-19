package etlflow.jdbc

import cats.effect.Blocker
import com.zaxxer.hikari.HikariConfig
import doobie.hikari.HikariTransactor
import etlflow.Credential.JDBC
import etlflow.DBEnv
import org.flywaydb.core.Flyway
import org.slf4j.{Logger, LoggerFactory}
import zio.blocking.Blocking
import zio.interop.catz._
import zio.{Task, ZIO, ZLayer}

trait DbManager {
  def liveTransactor(db: JDBC, pool_name: String = "EtlFlow-Pool", pool_size: Int = 2): ZLayer[Blocking, Throwable, DBEnv] = ZLayer.fromManaged {
    val config = new HikariConfig()
    config.setDriverClassName(db.driver)
    config.setJdbcUrl(db.url)
    config.setUsername(db.user)
    config.setPassword(db.password)
    config.setMaximumPoolSize(pool_size)
    config.setPoolName(pool_name)
    for {
      rt          <- Task.runtime.toManaged_
      blocker     <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
      transactor  <- HikariTransactor.fromHikariConfig[Task](config, rt.platform.executor.asEC, blocker).toManagedZIO
    } yield transactor
  }
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
}
