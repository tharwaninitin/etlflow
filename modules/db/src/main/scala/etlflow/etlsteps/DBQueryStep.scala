package etlflow.etlsteps

import etlflow.db.{DBApi, DBEnv}
import zio.RIO

class DBQueryStep private (val name: String, query: => String) extends EtlStep[DBEnv, Unit] {

  final def process: RIO[DBEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting DB Query Step: $name")
    logger.info(s"Query: $query")
    DBApi.executeQuery(query)
  }

  override def getStepProperties: Map[String, String] = Map("query" -> query)
}

object DBQueryStep {
  def apply(name: String, query: => String): DBQueryStep = new DBQueryStep(name, query)
}
