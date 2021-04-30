package etlflow

import etlflow.Credential.JDBC
import etlflow.api.{APIEnv, Implementation}
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.jdbc.{DBServerEnv, DbManager, liveDBWithTransactor}
import etlflow.executor.Executor
import etlflow.utils.{CacheHelper, Config, EtlFlowUtils, UtilityFunctions => UF}
import etlflow.webserver.Authentication
import io.circe.generic.auto._
import scalacache.caffeine.CaffeineCache
import zio.blocking.Blocking
import zio.{Chunk, Fiber, Queue, Runtime, Semaphore, Supervisor, ZLayer}

trait ServerSuiteHelper extends DbManager with EtlFlowUtils {

  type MEJP = MyEtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]]
  val cache: CaffeineCache[String] = CacheHelper.createCache[String]
  val config: Config = io.circe.config.parser.decode[Config]().toOption.get
  val credentials: JDBC = config.dbLog
  val ejpm_package: String = UF.getJobNamePackage[MEJP] + "$"
  val queue: Queue[(String, String, String, String)] = Runtime.default.unsafeRun(Queue.unbounded[(String,String,String,String)])
  val sem: Map[String, Semaphore] = Map("Job1" -> Runtime.default.unsafeRun(Semaphore.make(1)))
  val auth: Authentication = Authentication(authEnabled = true, cache, config.webserver)
  val executor: Executor[MEJP] = Executor[MEJP](sem, config, ejpm_package, queue)
  val supervisor: Supervisor[Chunk[Fiber.Runtime[Any, Any]]] = Runtime.default.unsafeRun(Supervisor.track(true))
  val testAPILayer: ZLayer[Blocking, Throwable, APIEnv] = Implementation.live[MEJP](auth, executor, List.empty, ejpm_package, supervisor)
  val testDBLayer: ZLayer[Blocking, Throwable, DBServerEnv with DBEnv] = liveDBWithTransactor(config.dbLog)

}
