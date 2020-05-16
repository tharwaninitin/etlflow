package etljobs.jdbc

import doobie.hikari.HikariTransactor
import org.apache.log4j.Logger
import zio.{Managed, Task}
import doobie.implicits._
import doobie.util.fragment.Fragment
import zio.interop.catz._

object QueryApi {
  private val query_logger = Logger.getLogger(getClass.getName)
  query_logger.info(s"Loaded ${getClass.getName}")

  def executeQuery(db: Managed[Throwable, HikariTransactor[Task]], query: String): Task[Unit] = {
    db.use{ transactor =>
      for {
        n <- Fragment.const(query).update.run.transact(transactor)
        _ <- Task(query_logger.info(n))
      } yield ()
    }
  }
}