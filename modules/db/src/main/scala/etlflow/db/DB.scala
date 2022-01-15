package etlflow.db

import etlflow.utils.ApplicationLogger
import etlflow.utils.EtlflowError.DBException
import scalikejdbc.{NamedDB, WrappedResultSet}
import zio._

private[etlflow] object DB extends ApplicationLogger {
  val live: ZLayer[Has[String], Throwable, DBEnv] = ZLayer.fromService { pool_name =>
    new DBApi.Service {
      override def executeQuery(query: String): IO[DBException, Unit] =
        Task(NamedDB(pool_name).localTx { implicit s =>
          scalikejdbc
            .SQL(query)
            .update()
        }).mapError({ e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
        }).unit
      override def executeQuerySingleOutput[T](query: String)(fn: WrappedResultSet => T): IO[DBException, T] =
        Task(NamedDB(pool_name).localTx { implicit s =>
          scalikejdbc
            .SQL(query)
            .map(fn)
            .single()
            .get
        }).mapError({ e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
        })
      override def executeQueryListOutput[T](query: String)(fn: WrappedResultSet => T): IO[DBException, List[T]] =
        Task(NamedDB(pool_name).localTx { implicit s =>
          scalikejdbc
            .SQL(query)
            .map(fn)
            .list()
        }).mapError({ e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
        })
    }
  }
}
