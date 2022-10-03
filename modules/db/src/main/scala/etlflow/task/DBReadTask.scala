package etlflow.task

import etlflow.db.{DBApi, DBEnv}
import scalikejdbc.WrappedResultSet
import zio.{RIO, ZIO}

class DBReadTask[T] private (val name: String, query: => String)(fn: WrappedResultSet => T) extends EtlTask[DBEnv, List[T]] {
  override protected def process: RIO[DBEnv, List[T]] = for {
    _   <- ZIO.logInfo("#" * 100)
    _   <- ZIO.logInfo(s"Starting DB Query Result Task: $name")
    _   <- ZIO.logInfo(s"Query: $query")
    out <- DBApi.executeQueryListOutput[T](query)(fn)
  } yield out
  override def getTaskProperties: Map[String, String] = Map("query" -> query)
}

object DBReadTask {
  def apply[T](name: String, query: => String)(fn: WrappedResultSet => T): DBReadTask[T] = new DBReadTask[T](name, query)(fn)
}
