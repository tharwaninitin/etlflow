package etlflow

import cats.effect.Blocker
import doobie.hikari.HikariTransactor
import etlflow.jdbc.DbManager
import etlflow.utils.Executor.DATAPROC
import etlflow.utils.{CacheHelper, Config, JDBC}
import io.circe.generic.auto._
import scalacache.Cache
import zio.Task
import scala.concurrent.ExecutionContext

trait SchedulerSuiteHelper extends DbManager {

  val cache: Cache[String] = CacheHelper.createCache[String](60)
  val global_properties: Config = io.circe.config.parser.decode[Config]().toOption.get
  val credentials: JDBC = global_properties.dbLog
  val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
  val transactor: HikariTransactor[Task] = zio.Runtime.default.unsafeRun(createDbTransactor(credentials,ec,Blocker.liftExecutionContext(ec), "EtlFlow-Scheduler-Testing-Pool"))
  val etlJob_name_package: String = "etlflow.MyEtlJobName$"

  lazy val dp_dep_libs: String = sys.env("DP_LIBS")
  lazy val dp_main_class: String = sys.env("DP_MAIN_CLASS")
  lazy val dp_libs: List[String] = dp_dep_libs.split(",").toList
  lazy val dataproc: DATAPROC = DATAPROC(
    sys.env("DP_PROJECT_ID"),
    sys.env("DP_REGION"),
    sys.env("DP_ENDPOINT"),
    sys.env("DP_CLUSTER_NAME")
  )
}