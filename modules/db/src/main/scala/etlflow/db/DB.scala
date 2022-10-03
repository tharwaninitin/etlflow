package etlflow.db

import etlflow.utils.ApplicationLogger
import etlflow.model.EtlFlowException.DBException
import scalikejdbc.{NamedDB, WrappedResultSet}
import zio._

private[etlflow] object DB extends ApplicationLogger {
  case class DBLive(poolName: String) extends DBApi.Service {
    override def executeQuery(query: String): IO[DBException, Unit] =
      ZIO
        .attempt(NamedDB(poolName).localTx { implicit s =>
          scalikejdbc
            .SQL(query)
            .update()
        })
        .mapError { e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
        }
        .unit
    @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
    override def executeQuerySingleOutput[T](query: String)(fn: WrappedResultSet => T): IO[DBException, T] =
      ZIO
        .attempt(NamedDB(poolName).localTx { implicit s =>
          scalikejdbc
            .SQL(query)
            .map(fn)
            .single()
            .get
        })
        .mapError { e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
        }
    override def executeQueryListOutput[T](query: String)(fn: WrappedResultSet => T): IO[DBException, List[T]] =
      ZIO
        .attempt(NamedDB(poolName).localTx { implicit s =>
          scalikejdbc
            .SQL(query)
            .map(fn)
            .list()
        })
        .mapError { e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
        }
  }

  val live: URLayer[String, DBLive] = ZLayer {
    for {
      poolName <- ZIO.service[String]
    } yield DBLive(poolName)
  }
}
