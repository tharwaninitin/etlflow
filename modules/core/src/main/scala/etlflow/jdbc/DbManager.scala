package etlflow.jdbc

import cats.effect.Blocker
import com.zaxxer.hikari.HikariConfig
import doobie.hikari.HikariTransactor
import doobie.util.fragment.Fragment
import etlflow.utils.JsonJackson
import org.flywaydb.core.Flyway
import org.slf4j.{Logger, LoggerFactory}
import zio.interop.catz._
import zio.{Managed, Task}
import scala.concurrent.ExecutionContext
import doobie.implicits._
import etlflow.Credential.JDBC

trait DbManager {

  def createDbTransactorManaged(credentials: JDBC, ec: ExecutionContext, pool_name: String = "LoggerPool", pool_size: Int = 2)
  (implicit blocker: Blocker = Blocker.liftExecutionContext(ec)): Managed[Throwable, HikariTransactor[Task]] = {
    val config = new HikariConfig()
    config.setDriverClassName(credentials.driver)
    config.setJdbcUrl(credentials.url)
    config.setUsername(credentials.user)
    config.setPassword(credentials.password)
    config.setMaximumPoolSize(pool_size)
    config.setPoolName(pool_name)
    HikariTransactor.fromHikariConfig[Task](config, ec, blocker)
  }.toManagedZIO

  def getDbCredentials[T : Manifest](name: String, credentials: JDBC, ec: ExecutionContext): Task[T] = {
    val query = s"SELECT value FROM credential WHERE name='$name' and valid_to is null;"
    createDbTransactorManaged(credentials,ec,"credential-pool",1).use { transactor =>
      for {
        result <- Fragment.const(query).query[String].unique.transact(transactor)
        op     <- Task(JsonJackson.convertToObject[T](result))
      } yield op
    }
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
