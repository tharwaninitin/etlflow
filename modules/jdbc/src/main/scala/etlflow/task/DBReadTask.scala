package etlflow.task

import etlflow.db.DB
import scalikejdbc.WrappedResultSet
import zio.{RIO, ZIO}

case class DBReadTask[T](name: String, query: String)(fn: WrappedResultSet => T) extends EtlTask[DB, Iterable[T]] {
  override protected def process: RIO[DB, Iterable[T]] = for {
    _   <- ZIO.logInfo("#" * 100)
    _   <- ZIO.logInfo(s"Starting DB Read Task: $name")
    _   <- ZIO.logInfo(s"Query: $query")
    out <- DB.fetchResults[T](query)(fn)
  } yield out
  override def getMetaData: Map[String, String] = Map("query" -> query)
}
