package etlflow

import etlflow.api.Schema.QueueDetails
import etlflow.cache.{CacheApi, CacheEnv}
import etlflow.db.{EtlJob, liveLogServerDB}
import etlflow.executor.Executor
import etlflow.json.JsonEnv
import etlflow.scheduler.Scheduler
import etlflow.schema.Config
import etlflow.utils.{Configuration, SetTimeZone, ReflectAPI => RF}
import etlflow.webserver.{Authentication, HttpServer}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock

abstract class ServerApp[T <: EJPMType : Tag]
  extends EtlFlowApp[T] with HttpServer with Scheduler {

  final private def createSemaphores(jobs: List[EtlJob]): Task[Map[String, Semaphore]] = {
    for {
      rt          <- Task.runtime
      semaphores  = jobs.map(job => (job.name, rt.unsafeRun(Semaphore.make(permits = job.props("job_max_active_runs").toLong)))).toMap
    } yield semaphores
  }

  def serverRunner(config: Config): ZIO[ZEnv, Throwable, Unit] = (for {
    _           <- SetTimeZone(config).toManaged_
    statsCache  <- CacheApi.createCache[QueueDetails].toManaged_
    authCache   <- CacheApi.createCache[String].toManaged_
    listTkn     = config.token.getOrElse(List.empty)
    _           <- ZIO.foreach_(listTkn)(tkn => CacheApi.put(authCache, tkn, tkn)).toManaged_
    auth        = Authentication(authCache, config.secretkey)
    jobs        <- RF.getJobs[T].toManaged_
    sem         <- createSemaphores(jobs).toManaged_
    executor    = Executor[T](sem, config, statsCache)
    supervisor  <- Supervisor.track(true).toManaged_
    dbLayer     = liveLogServerDB(config.db.get, pool_size = 10)
    logLayer    = log.Implementation.live
    cryptoLayer = crypto.Implementation.live(config.secretkey)
    apiLayer    = api.Implementation.live[T](auth,executor,jobs,supervisor,statsCache)
    finalLayer   = apiLayer ++ dbLayer ++ cryptoLayer ++ logLayer
    scheduler   = etlFlowScheduler(jobs).supervised(supervisor)
    webserver   = etlFlowWebServer(auth, config.webserver)
    _           <- scheduler.zipPar(webserver).provideSomeLayer[CacheEnv with JsonEnv with Blocking with Clock](finalLayer).toManaged_
  } yield ()).useNow.provideCustomLayer(cache.Implementation.live ++ json.Implementation.live)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = Configuration.config.flatMap(cfg => cliRunner(args,cfg,serverRunner(cfg))).exitCode
}
