package etlflow

import etlflow.Credential.JDBC
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.jdbc.{DBEnv, DbManager, liveDBWithTransactor}
import etlflow.utils.EtlFlowHelper.{CronJob, EtlJob, GQLEnv}
import etlflow.utils.{CacheHelper, Config, EtlFlowUtils, UtilityFunctions => UF}
import etlflow.webserver.api.ApiImplementation
import io.circe.generic.auto._
import scalacache.caffeine.CaffeineCache
import zio.blocking.Blocking
import zio.{Queue, Ref, Runtime, Semaphore, ZLayer}

trait ServerSuiteHelper extends DbManager with EtlFlowUtils {

  type MEJP = MyEtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]]
  val cache: CaffeineCache[String] = CacheHelper.createCache[String]
  val config: Config = io.circe.config.parser.decode[Config]().toOption.get
  val credentials: JDBC = config.dbLog
  val etlJob_name_package: String = UF.getJobNamePackage[MyEtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]]] + "$"
  val testJobsQueue: Queue[(String, String, String, String)] = Runtime.default.unsafeRun(Queue.unbounded[(String,String,String,String)])
  val testCronJobs: Ref[List[CronJob]] = Runtime.default.unsafeRun(Ref.make(List.empty[CronJob]))
  val testJobsSemaphore: Map[String, Semaphore] = Runtime.default.unsafeRun(createSemaphores(List(EtlJob("Job1",Map("job_max_active_runs" -> "1")))))
  val testAPILayer: ZLayer[Blocking, Throwable, GQLEnv] = ApiImplementation.live[MEJP](cache,testCronJobs,testJobsSemaphore,List.empty,testJobsQueue,config)
  val testDBLayer: ZLayer[Blocking, Throwable, DBEnv] = liveDBWithTransactor(config.dbLog)

}
