package etlflow.task

import etlflow.db.{DBApi, DBEnv}
import zio.RIO

class DBQueryTask private (val name: String, query: => String) extends EtlTaskZIO[DBEnv, Unit] {
  override protected def processZio: RIO[DBEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting DB Query Step: $name")
    logger.info(s"Query: $query")
    DBApi.executeQuery(query)
  }
  override def getStepProperties: Map[String, String] = Map("query" -> query)
}

object DBQueryTask {
  def apply(name: String, query: => String): DBQueryTask = new DBQueryTask(name, query)
}
