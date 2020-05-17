package etlflow.etlsteps

import doobie.hikari.HikariTransactor
import etlflow.jdbc.{DbManager, QueryApi}
import etlflow.utils.JDBC
import zio.{BootstrapRuntime, Managed, Task}

case class DBQueryStep private[etlflow](name: String, query: String, credentials: JDBC)
  extends EtlStep[Unit,Unit]
  with BootstrapRuntime
  with DbManager{

  val db: Managed[Throwable, HikariTransactor[Task]] =
    createDbTransactorManagedJDBC(credentials, platform.executor.asEC,  name + "-Pool")

  def process(in: Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting DB Query Step: $name")
    etl_logger.info(s"Query: $query")
    QueryApi.executeQuery(db, query)
  }

  override def getStepProperties(level: String): Map[String, String] = Map("query" -> query)
}


