package etlflow.db

import etlflow.audit.Audit
import etlflow.model.Credential.JDBC
import scalikejdbc.WrappedResultSet
import zio.{RIO, Task, TaskLayer, URLayer, ZIO, ZLayer}

trait DB {
  def executeQuery(query: String): Task[Unit]
  def executeQuerySingleOutput[T](query: String)(fn: WrappedResultSet => T): Task[T]
  def executeQueryListOutput[T](query: String)(fn: WrappedResultSet => T): Task[List[T]]
}

object DB {
  def executeQuery(query: String): RIO[DB, Unit] = ZIO.environmentWithZIO(_.get.executeQuery(query))

  def executeQuerySingleOutput[T](query: String)(fn: WrappedResultSet => T): RIO[DB, T] =
    ZIO.environmentWithZIO(_.get.executeQuerySingleOutput(query)(fn))

  def executeQueryListOutput[T](query: String)(fn: WrappedResultSet => T): RIO[DB, List[T]] =
    ZIO.environmentWithZIO(_.get.executeQueryListOutput(query)(fn))

  val layer: URLayer[String, DB] = ZLayer(ZIO.service[String].map(pool => DBImpl(pool)))

  def live(db: JDBC, poolName: String = "EtlFlow-DB-Pool", poolSize: Int = 2): TaskLayer[DB] =
    CP.layer(db, poolName, poolSize) >>> layer

  def liveAudit(
      db: JDBC,
      jobRunId: String,
      poolName: String = "EtlFlow-DB-Audit-Pool",
      poolSize: Int = 2
  ): TaskLayer[DB with Audit] =
    CP.layer(db, poolName, poolSize) >>> (DB.layer ++ etlflow.audit.DB.layer(jobRunId))
}
