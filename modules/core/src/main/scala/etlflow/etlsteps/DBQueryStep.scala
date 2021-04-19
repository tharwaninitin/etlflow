package etlflow.etlsteps

import etlflow.Credential.JDBC
import etlflow.jdbc.{DbManager, QueryApi}
import etlflow.utils.LoggingLevel
import zio.RIO
import zio.blocking.Blocking

class DBQueryStep private[etlflow](val name: String, query: => String, credentials: JDBC, pool_size: Int = 2)
  extends EtlStep[Unit,Unit]
    with DbManager {

  final def process(in: =>Unit): RIO[Blocking,Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting DB Query Step: $name")
    etl_logger.info(s"Query: $query")
    QueryApi.executeQuery(query).provideLayer(liveTransactor(credentials,name + "-Pool",pool_size))
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> query)
}

object DBQueryStep {
  def apply(name: String, query: => String, credentials: JDBC, pool_size: Int = 2): DBQueryStep =
    new DBQueryStep(name, query, credentials, pool_size)
}
