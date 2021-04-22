package etlflow.coretests

import cats.effect.Blocker
import doobie.Transactor
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor.Aux
import etlflow.jdbc.DbManager
import etlflow.log.ApplicationLogger
import etlflow.utils.Configuration
import zio.Task
import zio.interop.catz._

trait TestSuiteHelper extends Configuration with DbManager with ApplicationLogger {
  val canonical_path = new java.io.File(".").getCanonicalPath
  val file           = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"
  val testDBLayer    = liveTransactor(config.dbLog)
}

trait DoobieHelper {
  def transactor(url: String, user: String, pwd: String): Aux[Task, Unit]
  = Transactor.fromDriverManager[Task](
    "org.postgresql.Driver",     // driver classname
    url,        // connect URL (driver-specific)
    user,       // user
    pwd,        // password
    Blocker.liftExecutionContext(ExecutionContexts.synchronous) // just for testing
  )
}
