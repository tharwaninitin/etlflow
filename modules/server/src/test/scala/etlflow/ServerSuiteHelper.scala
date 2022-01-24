package etlflow

import cache4s.Cache
import crypto4s.Crypto
import etlflow.server.APIEnv
import etlflow.db.{DBEnv, DBServerEnv}
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.executor.ServerExecutor
import etlflow.jobtests.MyEtlJobPropsMapping
import etlflow.json.JsonEnv
import etlflow.log.LogEnv
import etlflow.model.Credential.JDBC
import etlflow.utils.Configuration
import etlflow.webserver.Authentication
import zio.blocking.Blocking
import zio.{Runtime, Semaphore, ZLayer}
import java.util.UUID

trait ServerSuiteHelper {
  val config = zio.Runtime.default.unsafeRun(Configuration.config)

  type MEJP = MyEtlJobPropsMapping[EtlJobProps, CoreEtlJob[EtlJobProps]]

  val authCache: Cache[String, String] = Cache.create[String, String]()
  val crypto: Crypto                   = Crypto(None)
  val credentials: JDBC                = config.db.get

  val sem: Map[String, Semaphore] =
    Map(
      "Job1" -> Runtime.default.unsafeRun(Semaphore.make(1)),
      "Job6" -> Runtime.default.unsafeRun(Semaphore.make(1)),
      "Job7" -> Runtime.default.unsafeRun(Semaphore.make(1)),
      "Job8" -> Runtime.default.unsafeRun(Semaphore.make(1)),
      "Job9" -> Runtime.default.unsafeRun(Semaphore.make(1))
    )

  val auth: Authentication           = Authentication(authCache, config.secretkey)
  val executor: ServerExecutor[MEJP] = ServerExecutor[MEJP](sem, config)

  type AllDBEnv = DBEnv with LogEnv with DBServerEnv
  val testAPILayer: ZLayer[Blocking, Throwable, APIEnv]   = server.Implementation.live[MEJP](auth, executor, List.empty, crypto)
  val testDBLayer: ZLayer[Blocking, Throwable, AllDBEnv]  = db.liveFullDBWithLog(config.db.get, UUID.randomUUID.toString)
  val testJsonLayer: ZLayer[Blocking, Throwable, JsonEnv] = json.Implementation.live

  val fullLayerWithoutDB = testAPILayer ++ testJsonLayer
  val fullLayer          = testAPILayer ++ testDBLayer ++ testJsonLayer
}
