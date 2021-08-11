package etlflow.etlsteps

import etlflow.db.{DBApi, liveDB}
import etlflow.schema.Credential.JDBC
import etlflow.schema.LoggingLevel
import scalikejdbc.WrappedResultSet
import zio.RIO
import zio.blocking.Blocking

class DBReadStep[T] private(val name: String, query: => String, credentials: JDBC, pool_size: Int = 2)(fn: WrappedResultSet => T)
  extends EtlStep[Unit,List[T]] {

  final def process(in: =>Unit): RIO[Blocking, List[T]]  = {
    logger.info("#"*100)
    logger.info(s"Starting DB Query Result Step: $name")
    logger.info(s"Query: $query")
    DBApi.executeQueryListOutput[T](query)(fn).provideLayer(liveDB(credentials, name + "-Pool", pool_size))
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> query)
}


object DBReadStep {
  def apply[T] (name: String, query: => String, credentials: JDBC, pool_size: Int = 2)(fn: WrappedResultSet => T): DBReadStep[T] =
    new DBReadStep[T](name, query, credentials, pool_size)(fn)
}