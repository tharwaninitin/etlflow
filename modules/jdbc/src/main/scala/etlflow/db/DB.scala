package etlflow.db

import etlflow.audit.Audit
import etlflow.model.Credential.JDBC
import scalikejdbc.{NoExtractor, SQL, WrappedResultSet}
import zio.{RIO, Task, TaskLayer, URLayer, ZIO, ZLayer}

trait DB {
  def executeQuery(query: String): Task[Unit]
  def executeQuery(query: SQL[Nothing, NoExtractor]): Task[Unit]
  def fetchResult[T](query: String)(fn: WrappedResultSet => T): Task[T]
  def fetchResults[T](query: String)(fn: WrappedResultSet => T): Task[Iterable[T]]
}

object DB {
  def executeQuery(query: String): RIO[DB, Unit] = ZIO.environmentWithZIO(_.get.executeQuery(query))

  def executeQuery(query: SQL[Nothing, NoExtractor]): RIO[DB, Unit] = ZIO.environmentWithZIO(_.get.executeQuery(query))

  def fetchResult[T](query: String)(fn: WrappedResultSet => T): RIO[DB, T] =
    ZIO.environmentWithZIO(_.get.fetchResult(query)(fn))

  def fetchResults[T](query: String)(fn: WrappedResultSet => T): RIO[DB, Iterable[T]] =
    ZIO.environmentWithZIO(_.get.fetchResults(query)(fn))

  def layer(fetchSize: Option[Int]): URLayer[String, DB] = ZLayer(ZIO.service[String].map(pool => DBImpl(pool, fetchSize)))

  def live(db: JDBC, poolName: String = "EtlFlow-DB-Pool", poolSize: Int = 2, fetchSize: Option[Int] = None): TaskLayer[DB] =
    CP.layer(db, poolName, poolSize) >>> layer(fetchSize)

  def liveAudit(
      db: JDBC,
      jobRunId: String,
      poolName: String = "EtlFlow-DB-Audit-Pool",
      poolSize: Int = 2,
      fetchSize: Option[Int] = None
  ): TaskLayer[DB with Audit] =
    CP.layer(db, poolName, poolSize) >>> (DB.layer(fetchSize) ++ etlflow.audit.DB.layer(jobRunId, fetchSize))
}
