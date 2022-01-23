package etlflow.etlsteps

import etlflow.db.{DBApi, DBEnv}
import scalikejdbc.WrappedResultSet
import zio.RIO

class DBReadStep[T] private (val name: String, query: => String)(fn: WrappedResultSet => T)
    extends EtlStep[DBEnv, List[T]] {

  final def process: RIO[DBEnv, List[T]] = {
    logger.info("#" * 100)
    logger.info(s"Starting DB Query Result Step: $name")
    logger.info(s"Query: $query")
    DBApi.executeQueryListOutput[T](query)(fn)
  }

  override def getStepProperties: Map[String, String] = Map("query" -> query)
}

object DBReadStep {
  def apply[T](name: String, query: => String)(fn: WrappedResultSet => T): DBReadStep[T] =
    new DBReadStep[T](name, query)(fn)
}
