package etlflow.db

import etlflow.schema.Credential.JDBC
import etlflow.utils.ApplicationLogger
import org.flywaydb.core.Flyway
import zio.Task

private[etlflow] object RunDbMigration extends ApplicationLogger{
  def apply(credentials: JDBC, clean: Boolean = false): Task[Unit] = Task {
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
