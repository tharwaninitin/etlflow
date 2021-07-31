package etlflow.etlsteps

import etlflow.db.{DBApi, liveDBWithTransactor}
import etlflow.schema.Credential.JDBC
import etlflow.schema.LoggingLevel
import zio.RIO
import zio.blocking.Blocking

class DBQueryStep private(val name: String, query: => String, credentials: JDBC, pool_size: Int = 2)
  extends EtlStep[Unit,Unit] {

  final def process(in: =>Unit): RIO[Blocking, Unit] = {
    logger.info("#"*100)
    logger.info(s"Starting DB Query Step: $name")
    logger.info(s"Query: $query")
    DBApi.executeQuery(query).provideLayer(liveDBWithTransactor(credentials, name + "-Pool", pool_size))
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> query)
}

object DBQueryStep {
  def apply(name: String, query: => String, credentials: JDBC, pool_size: Int = 2): DBQueryStep =
    new DBQueryStep(name, query, credentials, pool_size)
}
