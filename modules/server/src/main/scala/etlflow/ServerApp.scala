package etlflow

import etlflow.api.Schema.QueueDetails
import etlflow.db.{EtlJob, liveDBWithTransactor}
import etlflow.executor.Executor
import etlflow.scheduler.Scheduler
import etlflow.utils.{CacheHelper, EtlFlowUtils, SetTimeZone}
import etlflow.webserver.{Authentication, HttpServer}
import zio._

import scala.reflect.runtime.universe.TypeTag

abstract class ServerApp[EJN <: EJPMType : TypeTag]
  extends EtlFlowApp[EJN] with HttpServer with Scheduler with EtlFlowUtils  {

  final private def createSemaphores(jobs: List[EtlJob]): Task[Map[String, Semaphore]] = {
    for {
      rt          <- Task.runtime
      semaphores  = jobs.map(job => (job.name, rt.unsafeRun(Semaphore.make(permits = job.props("job_max_active_runs").toLong)))).toMap
    } yield semaphores
  }

  val serverRunner: ZIO[ZEnv, Throwable, Unit] = (for {
    _           <- SetTimeZone(config).toManaged_
    statsCache  = CacheHelper.createCache[QueueDetails]
    authCache   = CacheHelper.createCache[String]
    _           = config.token.map(_.foreach(tkn => CacheHelper.putKey(authCache,tkn,tkn)))
    auth        = Authentication(authCache, config.webserver)
    jsonLayer   = json.Implementation.live
    jobs        <- getEtlJobs[EJN](etl_job_props_mapping_package).provideCustomLayer(jsonLayer).toManaged_
    sem         <- createSemaphores(jobs).toManaged_
    executor    = Executor[EJN](sem, config, etl_job_props_mapping_package, statsCache)
    dbLayer     = liveDBWithTransactor(config.db)
    supervisor  <- Supervisor.track(true).toManaged_
    apiLayer    = api.Implementation.live[EJN](auth,executor,jobs,etl_job_props_mapping_package,supervisor,statsCache)
    finalLayer  = apiLayer ++ dbLayer ++ jsonLayer
    scheduler   = etlFlowScheduler(jobs).supervised(supervisor)
    webserver   = etlFlowWebServer(auth, config.webserver)
    _           <- scheduler.zipPar(webserver).provideCustomLayer(finalLayer).toManaged_
  } yield ()).useNow

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = cliRunner(args,serverRunner).exitCode
}
