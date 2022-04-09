package etlflow.task

import etlflow.db.{DBApi, DBEnv}
import scalikejdbc.WrappedResultSet
import zio.RIO

class DBReadTask[T] private (val name: String, query: => String)(fn: WrappedResultSet => T) extends EtlTask[DBEnv, List[T]] {
  override protected def process: RIO[DBEnv, List[T]] = {
    logger.info("#" * 100)
    logger.info(s"Starting DB Query Result Task: $name")
    logger.info(s"Query: $query")
    DBApi.executeQueryListOutput[T](query)(fn)
  }
  override def getTaskProperties: Map[String, String] = Map("query" -> query)
}

object DBReadTask {
  def apply[T](name: String, query: => String)(fn: WrappedResultSet => T): DBReadTask[T] = new DBReadTask[T](name, query)(fn)
}
