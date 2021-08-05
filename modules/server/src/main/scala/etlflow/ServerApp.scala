package etlflow

import etlflow.api.Schema.QueueDetails
import etlflow.db.{EtlJob, liveDBWithTransactor}
import etlflow.executor.Executor
import etlflow.scheduler.Scheduler
import etlflow.schema.{Config, LoggingLevel}
import etlflow.utils.{Configuration, EtlFlowUtils, SetTimeZone}
import etlflow.webserver.{Authentication, HttpServer}
import zio._
import scala.reflect.runtime.universe.TypeTag
import etlflow.cache.CacheApi
import etlflow.log.SlackLogger

abstract class ServerApp[EJN <: EJPMType : TypeTag]
  extends EtlFlowApp[EJN] with HttpServer with Scheduler with EtlFlowUtils  {

  final private def createSemaphores(jobs: List[EtlJob]): Task[Map[String, Semaphore]] = {
    for {
      rt          <- Task.runtime
      semaphores  = jobs.map(job => (job.name, rt.unsafeRun(Semaphore.make(permits = job.props("job_max_active_runs").toLong)))).toMap
    } yield semaphores
  }

  def serverRunner(config: Config): ZIO[ZEnv, Throwable, Unit] = (for {
    _           <- SetTimeZone(config).toManaged_
    cacheLayer  = cache.Implementation.live
    jsonLayer   = json.Implementation.live
    slack_logger = SlackLogger(config.slack)
    logLayer    = etlflow.log.Implementation.live(Some(slack_logger))
    cryptoLayer = crypto.Implementation.live(config.webserver.flatMap(_.secretKey))
    statsCache  <- CacheApi.createCache[QueueDetails].toManaged_
    authCache   <- CacheApi.createCache[String].toManaged_
    listTkn     = config.token.getOrElse(List.empty)
    _           <- ZIO.foreach_(listTkn)(tkn => CacheApi.put(authCache, tkn, tkn)).toManaged_
    auth        = Authentication(authCache, config.webserver)
    jobs        <- getEtlJobs[EJN](etl_job_props_mapping_package).provideCustomLayer(jsonLayer).toManaged_
    sem         <- createSemaphores(jobs).toManaged_
    executor    = Executor[EJN](sem, config, etl_job_props_mapping_package, statsCache)
    dbLayer     = liveDBWithTransactor(config.db)
    supervisor  <- Supervisor.track(true).toManaged_
    apiLayer    = api.Implementation.live[EJN](auth,executor,jobs,etl_job_props_mapping_package,supervisor,statsCache)
    finalLayer  = apiLayer ++ dbLayer ++ jsonLayer ++ cryptoLayer ++ cacheLayer ++ logLayer
    scheduler   = etlFlowScheduler(jobs).supervised(supervisor)
    webserver   = etlFlowWebServer(auth, config.webserver)
    _           <- scheduler.zipPar(webserver).provideCustomLayer(finalLayer).toManaged_
  } yield ()).useNow.provideCustomLayer(cache.Implementation.live)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = Configuration.config.flatMap(cfg => cliRunner(args,cfg,serverRunner(cfg))).exitCode
}
