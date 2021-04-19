package etlflow.etlsteps

import doobie.util.Read
import etlflow.Credential.JDBC
import etlflow.jdbc.{DbManager, QueryApi}
import etlflow.utils.LoggingLevel
import zio.RIO
import zio.blocking.Blocking

class DBReadStep[T <: Product : Read] private[etlflow](val name: String, query: => String, credentials: JDBC, pool_size: Int = 2)
  extends EtlStep[Unit,List[T]]
    with DbManager {

  final def process(in: =>Unit): RIO[Blocking,List[T]] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting DB Query Result Step: $name")
    etl_logger.info(s"Query: $query")
    QueryApi.executeQueryWithResponse[T](query)
      .provideLayer(liveTransactor(credentials, name + "-Pool", pool_size))
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> query)
}


object DBReadStep {
  def apply[T <: Product : Read] (name: String, query: => String, credentials: JDBC, pool_size: Int = 2): DBReadStep[T] =
    new DBReadStep[T](name, query, credentials, pool_size)
}