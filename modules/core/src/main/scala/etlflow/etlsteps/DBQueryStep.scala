package etlflow.etlsteps

import etlflow.jdbc.{DbManager, QueryApi}
import etlflow.Credential.JDBC
import etlflow.utils.LoggingLevel
import zio.Task

class DBQueryStep private[etlflow](
                                    val name: String,
                                    query: => String,
                                    credentials: JDBC
                                  )
  extends EtlStep[Unit,Unit]
    with DbManager{

  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting DB Query Step: $name")
    etl_logger.info(s"Query: $query")
    QueryApi.executeQuery(createDbTransactorManaged(credentials, scala.concurrent.ExecutionContext.Implicits.global,  name + "-Pool"), query)
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> query)
}

object DBQueryStep {
  def apply(name: String, query: => String, credentials: JDBC): DBQueryStep =
    new DBQueryStep(name, query, credentials)
}
