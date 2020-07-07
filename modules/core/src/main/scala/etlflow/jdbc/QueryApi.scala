package etlflow.jdbc

import doobie.hikari.HikariTransactor
import zio.{Managed, Task}
import doobie.implicits._
import doobie.util.Read
import doobie.util.fragment.Fragment
import org.slf4j.LoggerFactory
import zio.interop.catz._

object QueryApi {
  private val query_logger = LoggerFactory.getLogger(getClass.getName)
  query_logger.info(s"Loaded ${getClass.getName}")

  def executeQueryWithResponse[T <: Product : Read](db: Managed[Throwable, HikariTransactor[Task]], query: String): Task[List[T]] = {

    db.use{ transactor =>
      for {
        result <- Fragment.const(query).query[T].to[List].transact(transactor)
      } yield result
    }
  }

  def executeQuery(db: Managed[Throwable, HikariTransactor[Task]], query: String): Task[Unit] = {
    db.use{ transactor =>
      for {
        n <- Fragment.const(query).update.run.transact(transactor)
        _ <- Task(query_logger.info(n.toString))
      } yield ()
    }
  }
}