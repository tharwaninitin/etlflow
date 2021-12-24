package etlflow

import etlflow.api.Schema.QueueDetails
import etlflow.api.{APIEnv, Implementation}
import etlflow.cache.{CacheApi, CacheEnv}
import etlflow.crypto.CryptoEnv
import etlflow.db.{DBEnv, DBServerEnv, liveFullDB}
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.executor.Executor
import etlflow.jobtests.MyEtlJobPropsMapping
import etlflow.json.JsonEnv
import etlflow.log.LogEnv
import etlflow.schema.Credential.JDBC
import etlflow.utils.Configuration
import etlflow.webserver.Authentication
import zio.Runtime.default.unsafeRun
import zio.blocking.Blocking
import zio.{Chunk, Fiber, Runtime, Semaphore, Supervisor, ZLayer}

trait ServerSuiteHelper {
  val config = zio.Runtime.default.unsafeRun(Configuration.config)
  val skey = config.secretkey

  type MEJP = MyEtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]]

  val authCache: cache.Cache[String] = unsafeRun(CacheApi.createCache[String].provideCustomLayer(cache.Implementation.live))
  val jobStatsCache: cache.Cache[QueueDetails] = unsafeRun(CacheApi.createCache[QueueDetails].provideCustomLayer(cache.Implementation.live))

  val credentials: JDBC = config.db.get
  val sem: Map[String, Semaphore] =
    Map(
      "Job1" -> Runtime.default.unsafeRun(Semaphore.make(1)),
      "Job6" -> Runtime.default.unsafeRun(Semaphore.make(1)),
      "Job7" -> Runtime.default.unsafeRun(Semaphore.make(1)),
      "Job8" -> Runtime.default.unsafeRun(Semaphore.make(1)),
      "Job9" -> Runtime.default.unsafeRun(Semaphore.make(1)))

  val auth: Authentication = Authentication(authCache, config.secretkey)

  val executor: Executor[MEJP] = Executor[MEJP](sem, config, jobStatsCache)
  val supervisor: Supervisor[Chunk[Fiber.Runtime[Any, Any]]] = Runtime.default.unsafeRun(Supervisor.track(true))
  val testAPILayer: ZLayer[Blocking, Throwable, APIEnv] = Implementation.live[MEJP](auth, executor, List.empty, supervisor, jobStatsCache)
  val testDBLayer: ZLayer[Blocking, Throwable, DBEnv with LogEnv with DBServerEnv] = liveFullDB(config.db.get)
  val testJsonLayer: ZLayer[Blocking, Throwable, JsonEnv] = json.Implementation.live
  val testCryptoLayer: ZLayer[Blocking, Throwable, CryptoEnv] = crypto.Implementation.live(skey)
  val testCacheLayer: ZLayer[Blocking, Throwable, CacheEnv] = cache.Implementation.live
  val fullLayerWithoutDB  = testAPILayer ++ testJsonLayer ++ testCryptoLayer ++ testCacheLayer
  val fullLayer  = testAPILayer ++  testDBLayer ++ testJsonLayer ++ testCryptoLayer ++ testCacheLayer
}
