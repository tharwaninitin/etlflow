package etlflow.task

import etlflow.db.DB
import zio.{RIO, ZIO}

case class DBQueryTask(name: String, query: String) extends EtlTask[DB, Unit] {
  override protected def process: RIO[DB, Unit] = for {
    _ <- ZIO.logInfo("#" * 100)
    _ <- ZIO.logInfo(s"Starting DB Query Task: $name")
    _ <- ZIO.logInfo(s"Query: $query")
    _ <- DB.executeQuery(query)
  } yield ()
  override def getTaskProperties: Map[String, String] = Map("query" -> query)
}
