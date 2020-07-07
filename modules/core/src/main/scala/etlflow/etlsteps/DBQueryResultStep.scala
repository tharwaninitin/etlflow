package etlflow.etlsteps

import doobie.hikari.HikariTransactor
import doobie.util.Read
import etlflow.jdbc.{DbManager, QueryApi}
import etlflow.utils.JDBC
import zio.{Managed, Task}

class DBQueryResultStep[T <: Product : Read] private[etlflow](
                val name: String,
                query: => String,
                credentials: JDBC
              )
  extends EtlStep[Unit,List[T]]
  with DbManager{

  lazy val db: Managed[Throwable, HikariTransactor[Task]] =
    createDbTransactorManagedJDBC(credentials, scala.concurrent.ExecutionContext.Implicits.global,  name + "-Pool")

  final def process(in: =>Unit): Task[List[T]] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting DB Query Result Step: $name")
    etl_logger.info(s"Query: $query")
    QueryApi.executeQueryWithResponse[T](db, query)
  }

  override def getStepProperties(level: String): Map[String, String] = Map("query" -> query)
}


object DBQueryResultStep {
  def apply[T <: Product : Read]
    (name: String,
      query: => String,
      credentials: JDBC
    ): DBQueryResultStep[T] = {
    new DBQueryResultStep[T](name, query, credentials)
  }
}