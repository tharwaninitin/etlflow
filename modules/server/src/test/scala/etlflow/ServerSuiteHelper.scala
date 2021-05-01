package etlflow

import etlflow.Credential.JDBC
import etlflow.api.Schema.QueueDetails
import etlflow.api.{APIEnv, Implementation}
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.executor.Executor
import etlflow.jdbc.{DBServerEnv, DbManager, liveDBWithTransactor}
import etlflow.utils.{CacheHelper, Config, EtlFlowUtils, UtilityFunctions => UF}
import etlflow.webserver.Authentication
import io.circe.generic.auto._
import scalacache.caffeine.CaffeineCache
import zio.blocking.Blocking
import zio.{Chunk, Fiber, Runtime, Semaphore, Supervisor, ZLayer}

trait ServerSuiteHelper extends DbManager with EtlFlowUtils {

  type MEJP = MyEtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]]
  val authCache: CaffeineCache[String] = CacheHelper.createCache[String]
  val jobStatsCache: CaffeineCache[QueueDetails] = CacheHelper.createCache[QueueDetails]
  val config: Config = io.circe.config.parser.decode[Config]().toOption.get
  val credentials: JDBC = config.dbLog
  val ejpm_package: String = UF.getJobNamePackage[MEJP] + "$"
  val sem: Map[String, Semaphore] = Map("Job1" -> Runtime.default.unsafeRun(Semaphore.make(1)))
  val auth: Authentication = Authentication(authEnabled = true, authCache, config.webserver)
  val executor: Executor[MEJP] = Executor[MEJP](sem, config, ejpm_package, jobStatsCache)
  val supervisor: Supervisor[Chunk[Fiber.Runtime[Any, Any]]] = Runtime.default.unsafeRun(Supervisor.track(true))
  val testAPILayer: ZLayer[Blocking, Throwable, APIEnv] = Implementation.live[MEJP](auth, executor, List.empty, ejpm_package, supervisor, jobStatsCache)
  val testDBLayer: ZLayer[Blocking, Throwable, DBServerEnv with DBEnv] = liveDBWithTransactor(config.dbLog)

}
