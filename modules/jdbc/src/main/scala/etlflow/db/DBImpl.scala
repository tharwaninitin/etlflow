package etlflow.db

import etlflow.log.ApplicationLogger
import etlflow.model.EtlFlowException.DBException
import scalikejdbc.{NamedDB, NoExtractor, SQL, WrappedResultSet}
import zio._

private[etlflow] case class DBImpl(poolName: String, fetchSize: Option[Int]) extends DB with ApplicationLogger {
  override def executeQuery(query: String): IO[DBException, Unit] =
    ZIO
      .attempt(NamedDB(poolName).localTx(implicit s => scalikejdbc.SQL(query).update()))
      .mapError { e =>
        logger.error(e.getMessage)
        DBException(e.getMessage)
      }
      .unit
  override def executeQuery(query: SQL[Nothing, NoExtractor]): IO[DBException, Unit] =
    ZIO
      .attempt(NamedDB(poolName).localTx(implicit s => query.update()))
      .mapError { e =>
        logger.error(e.getMessage)
        DBException(e.getMessage)
      }
      .unit
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  override def fetchResult[T](query: String)(fn: WrappedResultSet => T): IO[DBException, T] =
    ZIO
      .attempt(NamedDB(poolName).localTx(implicit s => scalikejdbc.SQL(query).map(fn).single().get))
      .mapError { e =>
        logger.error(e.getMessage)
        DBException(e.getMessage)
      }
  override def fetchResults[T](query: String)(fn: WrappedResultSet => T): IO[DBException, Iterable[T]] =
    ZIO
      .attempt(NamedDB(poolName).localTx(implicit s => scalikejdbc.SQL(query).fetchSize(fetchSize).map(fn).iterable()))
      .mapError { e =>
        logger.error(e.getMessage)
        DBException(e.getMessage)
      }
}
