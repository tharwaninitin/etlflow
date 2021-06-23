package etlflow.etlsteps

import etlflow.JobEnv
import etlflow.db.{DBApi, liveDBWithTransactor}
import etlflow.schema.Credential.JDBC
import etlflow.utils.{Configuration, LoggingLevel}
import zio.RIO

class DBQueryStep private[etlflow](val name: String, query: => String, credentials: JDBC, pool_size: Int = 2)
  extends EtlStep[Unit,Unit]
    with Configuration{

  final def process(in: =>Unit): RIO[JobEnv, Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting DB Query Step: $name")
    etl_logger.info(s"Query: $query")
    DBApi.executeQuery(query).provideLayer(liveDBWithTransactor(credentials, name + "-Pool",pool_size))
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> query)
}

object DBQueryStep {
  def apply(name: String, query: => String, credentials: JDBC, pool_size: Int = 2): DBQueryStep =
    new DBQueryStep(name, query, credentials, pool_size)
}
