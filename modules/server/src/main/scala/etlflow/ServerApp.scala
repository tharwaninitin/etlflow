package etlflow

import etlflow.api.Implementation
import etlflow.api.Schema.EtlJob
import etlflow.executor.Executor
import etlflow.jdbc.liveDBWithTransactor
import etlflow.scheduler.Scheduler
import etlflow.utils.{CacheHelper, EtlFlowUtils, SetTimeZone}
import etlflow.webserver.{Authentication, Http4sServer}
import zio._
import scala.reflect.runtime.universe.TypeTag

abstract class ServerApp[EJN <: EJPMType : TypeTag]
  extends EtlFlowApp[EJN] with Http4sServer with Scheduler with EtlFlowUtils  {

  final private def createSemaphores(jobs: List[EtlJob]): Task[Map[String, Semaphore]] = {
    for {
      rt          <- Task.runtime
      semaphores  = jobs.map(job => (job.name, rt.unsafeRun(Semaphore.make(permits = job.props("job_max_active_runs").toLong)))).toMap
    } yield semaphores
  }
  
  final private def monitorFibers(supervisor: Supervisor[Chunk[Fiber.Runtime[Any, Any]]]): ZIO[Any, Nothing, Unit] = for {
    uio_status <- supervisor.value.map(_.map(_.dump))
    status     <- ZIO.collectAll(uio_status.toList)
    _          = logger.info(s"Scheduled fiber info")
    _          = status.foreach(x => logger.info(s"${x.fiberId} ${x.fiberName} ${x.status}"))
  } yield ()

  val serverRunner: ZIO[ZEnv, Throwable, Unit] = (for {
    _           <- SetTimeZone(config).toManaged_
    queue       <- Queue.sliding[(String,String,String,String)](10).toManaged_
    cache       = CacheHelper.createCache[String]
    _           = config.token.map(_.foreach(tkn => CacheHelper.putKey(cache,tkn,tkn)))
    auth        = Authentication(authEnabled = true, cache, config.webserver)
    jobs        <- getEtlJobs[EJN](etl_job_props_mapping_package).toManaged_
    sem         <- createSemaphores(jobs).toManaged_
    executor    = Executor[EJN](sem, config, etl_job_props_mapping_package, queue)
    dbLayer     = liveDBWithTransactor(config.dbLog)
    apiLayer    = Implementation.live[EJN](auth,executor,jobs,etl_job_props_mapping_package)
    finalLayer  = apiLayer ++ dbLayer
    supervisor  <- Supervisor.track(true).toManaged_
    //_           <- monitorFibers(supervisor).repeat(Schedule.spaced(60000.milliseconds)).toManaged_.fork
    scheduler   = etlFlowScheduler(jobs).supervised(supervisor)
    webserver   = etlFlowWebServer[EJN](auth,config.webserver)
    _           <- scheduler.zipPar(webserver).provideCustomLayer(finalLayer).toManaged_
  } yield ()).useNow

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = cliRunner(args,serverRunner).exitCode
}
