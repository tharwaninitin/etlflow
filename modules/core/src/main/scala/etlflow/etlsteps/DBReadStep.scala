package etlflow.etlsteps

import doobie.util.Read
import etlflow.jdbc.{DbManager, QueryApi}
import etlflow.utils.{JDBC, LoggingLevel}
import zio.Task

class DBReadStep[T <: Product : Read] private[etlflow](
                                                        val name: String,
                                                        query: => String,
                                                        credentials: JDBC
                                                      )
  extends EtlStep[Unit,List[T]]
    with DbManager {

  final def process(in: =>Unit): Task[List[T]] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting DB Query Result Step: $name")
    etl_logger.info(s"Query: $query")
    QueryApi.executeQueryWithResponse[T](createDbTransactorManaged(credentials, scala.concurrent.ExecutionContext.Implicits.global,  name + "-Pool"), query)
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> query)
}


object DBReadStep {
  def apply[T <: Product : Read] (name: String, query: => String, credentials: JDBC): DBReadStep[T] =
    new DBReadStep[T](name, query, credentials)
}