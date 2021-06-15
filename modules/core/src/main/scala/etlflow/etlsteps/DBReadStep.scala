package etlflow.etlsteps

import doobie.util.Read
import etlflow.JobEnv
import etlflow.jdbc.{DB, DBEnv, TransactorEnv}
import etlflow.schema.Credential.JDBC
import etlflow.utils.{DbManager, LoggingLevel}
import zio.blocking.Blocking
import zio.{RIO, ZLayer}

class DBReadStep[T <: Product : Read] private[etlflow](val name: String, query: => String, credentials: JDBC, pool_size: Int = 2)
  extends EtlStep[Unit,List[T]]
    with DbManager {

  private def liveDBWithTransactor(credentials: JDBC): ZLayer[Blocking, Throwable, TransactorEnv with DBEnv] =
    liveTransactor(credentials,name + "-Pool",pool_size) >+> DB.liveDB

  final def process(in: =>Unit):  RIO[JobEnv, List[T]]  = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting DB Query Result Step: $name")
    etl_logger.info(s"Query: $query")
    DB.executeQueryWithResponse[T](query).provideLayer(liveDBWithTransactor(credentials))
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> query)
}


object DBReadStep {
  def apply[T <: Product : Read] (name: String, query: => String, credentials: JDBC, pool_size: Int = 2): DBReadStep[T] =
    new DBReadStep[T](name, query, credentials, pool_size)
}