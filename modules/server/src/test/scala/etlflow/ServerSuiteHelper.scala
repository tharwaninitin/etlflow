package etlflow

import etlflow.api.Schema.QueueDetails
import etlflow.api.{APIEnv, Implementation}
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.db.{DBEnv, liveDBWithTransactor}
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.executor.Executor
import etlflow.json.JsonEnv
import etlflow.schema.Credential.JDBC
import etlflow.utils.{CacheHelper, Configuration, EtlFlowUtils, ReflectAPI => RF}
import etlflow.webserver.Authentication
import scalacache.caffeine.CaffeineCache
import zio.blocking.Blocking
import zio.{Chunk, Fiber, Runtime, Semaphore, Supervisor, ZLayer}


trait ServerSuiteHelper extends EtlFlowUtils with Configuration {

  type MEJP = MyEtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]]
  val authCache: CaffeineCache[String] = CacheHelper.createCache[String]
  val jobStatsCache: CaffeineCache[QueueDetails] = CacheHelper.createCache[QueueDetails]
  val credentials: JDBC = config.db
  val ejpm_package: String = RF.getJobNamePackage[MEJP] + "$"
  val sem: Map[String, Semaphore] =
    Map(
      "Job1" -> Runtime.default.unsafeRun(Semaphore.make(1)),
      "Job6" -> Runtime.default.unsafeRun(Semaphore.make(1)),
      "Job7" -> Runtime.default.unsafeRun(Semaphore.make(1)),
      "Job8" -> Runtime.default.unsafeRun(Semaphore.make(1)),
      "Job9" -> Runtime.default.unsafeRun(Semaphore.make(1)))

  val auth: Authentication = Authentication(authCache, config.webserver)

  val executor: Executor[MEJP] = Executor[MEJP](sem, config, ejpm_package, jobStatsCache)
  val supervisor: Supervisor[Chunk[Fiber.Runtime[Any, Any]]] = Runtime.default.unsafeRun(Supervisor.track(true))
  val testAPILayer: ZLayer[Blocking, Throwable, APIEnv] = Implementation.live[MEJP](auth, executor, List.empty, ejpm_package, supervisor, jobStatsCache)
  val testDBLayer: ZLayer[Blocking, Throwable, DBEnv] = liveDBWithTransactor(config.db)
  val testJsonLayer: ZLayer[Blocking, Throwable, JsonEnv] = json.Implementation.live
}
