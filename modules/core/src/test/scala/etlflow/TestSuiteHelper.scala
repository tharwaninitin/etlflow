package etlflow

import cats.effect.Blocker
import doobie.Transactor
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor.Aux
import etlflow.utils.Config
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.ConfigSource
import zio.interop.catz._
import zio.{Runtime, Task, ZEnv}
import pureconfig.generic.auto._

trait TestSuiteHelper {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val canonical_path: String    = new java.io.File(".").getCanonicalPath
  val file                      = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"
  val global_properties: Config = ConfigSource.default.loadOrThrow[Config]

  val runtime: Runtime[ZEnv]          = Runtime.default
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
