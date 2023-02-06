package etlflow.db

import etlflow.log.ApplicationLogger
import scalikejdbc.{NamedDB, NoExtractor, SQL, WrappedResultSet}
import zio._

private[etlflow] case class DBImpl(poolName: String, fetchSize: Option[Int]) extends DB with ApplicationLogger {

  override def executeQuery(query: String): Task[Unit] = ZIO
    .attempt(NamedDB(poolName).localTx(implicit s => scalikejdbc.SQL(query).update()))
    .tapError(e => ZIO.logError(e.getMessage))
    .unit

  override def executeQuery(query: SQL[Nothing, NoExtractor]): Task[Unit] = ZIO
    .attempt(NamedDB(poolName).localTx(implicit s => query.update()))
    .tapError(e => ZIO.logError(e.getMessage))
    .unit

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  override def fetchResult[T](query: String)(fn: WrappedResultSet => T): Task[T] = ZIO
    .attempt(NamedDB(poolName).localTx(implicit s => scalikejdbc.SQL(query).map(fn).single().get))
    .tapError(e => ZIO.logError(e.getMessage))

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def fetchResults[T](query: String)(fn: WrappedResultSet => T): Task[Iterable[T]] = ZIO
    .attempt(NamedDB(poolName).localTx(implicit s => scalikejdbc.SQL(query).fetchSize(fetchSize).map(fn).iterable()))
    .tapError(e => ZIO.logError(e.getMessage))
}
