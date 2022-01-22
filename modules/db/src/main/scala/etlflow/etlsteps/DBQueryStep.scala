package etlflow.etlsteps

import etlflow.db.{liveDB, DBApi}
import etlflow.model.Credential.JDBC
import zio.RIO
import zio.blocking.Blocking

class DBQueryStep private (val name: String, query: => String, credentials: JDBC, pool_size: Int)
    extends EtlStep[Unit] {

  final def process: RIO[Blocking, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting DB Query Step: $name")
    logger.info(s"Query: $query")
    DBApi.executeQuery(query).provideLayer(liveDB(credentials, name + "-Pool", pool_size))
  }

  override def getStepProperties: Map[String, String] = Map("query" -> query)
}

object DBQueryStep {
  def apply(name: String, query: => String, credentials: JDBC, pool_size: Int = 2): DBQueryStep =
    new DBQueryStep(name, query, credentials, pool_size)
}
