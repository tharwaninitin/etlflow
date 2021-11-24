package etlflow.db

import scalikejdbc.WrappedResultSet
import zio.{IO, ZIO}

object DBApi {
  trait Service {
    def executeQuery(query: String): IO[Throwable, Unit]
    def executeQuerySingleOutput[T](query: String)(fn: WrappedResultSet => T): IO[Throwable, T]
    def executeQueryListOutput[T](query: String)(fn: WrappedResultSet => T): IO[Throwable, List[T]]
  }
  def executeQuery(query: String): ZIO[DBEnv, Throwable, Unit] = ZIO.accessM(_.get.executeQuery(query))
  def executeQuerySingleOutput[T](query: String)(fn: WrappedResultSet => T):ZIO[DBEnv, Throwable, T] = ZIO.accessM(_.get.executeQuerySingleOutput(query)(fn))
  def executeQueryListOutput[T](query: String)(fn: WrappedResultSet => T):ZIO[DBEnv, Throwable, List[T]] = ZIO.accessM(_.get.executeQueryListOutput(query)(fn))
}
