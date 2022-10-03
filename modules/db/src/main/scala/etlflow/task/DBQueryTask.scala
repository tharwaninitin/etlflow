package etlflow.task

import etlflow.db.{DBApi, DBEnv}
import zio.{RIO, ZIO}

class DBQueryTask private (val name: String, query: => String) extends EtlTask[DBEnv, Unit] {
  override protected def process: RIO[DBEnv, Unit] = for {
    _ <- ZIO.logInfo("#" * 100)
    _ <- ZIO.logInfo(s"Starting DB Query Task: $name")
    _ <- ZIO.logInfo(s"Query: $query")
    _ <- DBApi.executeQuery(query)
  } yield ()
  override def getTaskProperties: Map[String, String] = Map("query" -> query)
}

object DBQueryTask {
  def apply(name: String, query: => String): DBQueryTask = new DBQueryTask(name, query)
}
