package etlflow.etlsteps

import etlflow.JobEnv
import etlflow.jdbc.{DB, DBEnv, TransactorEnv, liveDBWithTransactor, liveTransactor}
import etlflow.schema.Credential.JDBC
import etlflow.utils.{Configuration, DbManager, LoggingLevel}
import zio.{RIO, ZLayer}
import zio.blocking.Blocking

class DBQueryStep private[etlflow](val name: String, query: => String, credentials: JDBC, pool_size: Int = 2)
  extends EtlStep[Unit,Unit]
    with DbManager  with Configuration{

  private def liveDBWithTransactor(credentials: JDBC): ZLayer[Blocking, Throwable, TransactorEnv with DBEnv] =
    liveTransactor(credentials,name + "-Pool",pool_size) >+> DB.liveDB

  final def process(in: =>Unit): RIO[JobEnv, Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting DB Query Step: $name")
    etl_logger.info(s"Query: $query")
    DB.executeQuery(query).provideLayer(liveDBWithTransactor(credentials))
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> query)
}

object DBQueryStep {
  def apply(name: String, query: => String, credentials: JDBC, pool_size: Int = 2): DBQueryStep =
    new DBQueryStep(name, query, credentials, pool_size)
}
