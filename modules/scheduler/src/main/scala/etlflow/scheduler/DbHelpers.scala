package etlflow.scheduler

import cats.effect.{Blocker, Resource}
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import zio._
import zio.interop.catz._

object DbHelpers {
  def dbResource(db_driver: String, db_url: String, db_user: String, db_pass: String): Resource[Task, HikariTransactor[Task]] = {
    for {
      connectEC <- ExecutionContexts.fixedThreadPool[Task](10)
      xa        <- HikariTransactor.newHikariTransactor[Task](
        db_driver,  // driver classname
        db_url,     // connect URL
        db_user,    // username
        db_pass,    // password
        connectEC,                              // await connection here
        Blocker.liftExecutionContext(connectEC) // transactEC // execute JDBC operations here
      )
      } yield xa
    }
}
