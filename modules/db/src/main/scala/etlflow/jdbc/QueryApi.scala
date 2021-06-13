package etlflow.jdbc

import doobie.implicits._
import doobie.util.Read
import doobie.util.fragment.Fragment
import etlflow.utils.DbManager
import org.slf4j.{Logger, LoggerFactory}
import zio.interop.catz._
import zio.{RIO, Task, ZIO}

object QueryApi extends DbManager {

  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def executeQueryWithResponse[T <: Product : Read](query: String): RIO[DBEnv, List[T]] = ZIO.accessM[DBEnv] { x =>
    Fragment.const(query)
      .query[T]
      .to[List]
      .transact(x.get)
  }

  def executeQuery(query: String): RIO[DBEnv, Unit] = ZIO.accessM[DBEnv] { x =>
    for {
      n <- Fragment.const(query).update.run.transact(x.get)
      _ <- Task(logger.info(s"No of rows affected: $n"))
    } yield ()
  }

  def executeQueryWithSingleResponse[T : Read](query: String): RIO[DBEnv, T] = ZIO.accessM[DBEnv] { x =>
    Fragment.const(query)
      .query[T]
      .unique
      .transact(x.get)
  }
}