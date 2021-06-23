package etlflow.db

import etlflow.schema.Credential.JDBC
import org.flywaydb.core.Flyway
import org.slf4j.{Logger, LoggerFactory}
import zio.Task

private[etlflow] object RunDbMigration {
  def apply(credentials: JDBC, clean: Boolean = false): Task[Unit] = Task {
    val logger: Logger = LoggerFactory.getLogger(getClass.getName)
    val configuration = Flyway
      .configure(this.getClass.getClassLoader)
      .dataSource(credentials.url, credentials.user, credentials.password)
      .locations("migration")
      .connectRetries(10)
      .load()
    logger.info("Running db migration from paths:")
    logger.info(configuration.info().all().toList.map(x => x.getPhysicalLocation).mkString("\n", "\n", ""))
    if (clean) configuration.clean()
    configuration.migrate()
  }
}
