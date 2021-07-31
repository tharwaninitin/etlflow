package etlflow.etlsteps

import doobie.util.Read
import etlflow.db.{DBApi, liveDBWithTransactor}
import etlflow.schema.Credential.JDBC
import etlflow.schema.LoggingLevel
import zio.RIO
import zio.blocking.Blocking

class DBReadStep[T <: Product : Read] private(val name: String, query: => String, credentials: JDBC, pool_size: Int = 2)
  extends EtlStep[Unit,List[T]] {

  final def process(in: =>Unit): RIO[Blocking, List[T]]  = {
    logger.info("#"*100)
    logger.info(s"Starting DB Query Result Step: $name")
    logger.info(s"Query: $query")
    DBApi.executeQueryWithResponse[T](query).provideLayer(liveDBWithTransactor(credentials, name + "-Pool", pool_size))
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> query)
}


object DBReadStep {
  def apply[T <: Product : Read] (name: String, query: => String, credentials: JDBC, pool_size: Int = 2): DBReadStep[T] =
    new DBReadStep[T](name, query, credentials, pool_size)
}