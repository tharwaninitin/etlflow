package etlflow

import cache4s.Cache
import crypto4s.Crypto
import etlflow.server.model.EtlJob
import etlflow.executor.ServerExecutor
import etlflow.json.JsonEnv
import etlflow.scheduler.Scheduler
import etlflow.schema.Config
import etlflow.server.Implementation
import etlflow.utils.{Configuration, SetTimeZone, ReflectAPI => RF}
import etlflow.webserver.{Authentication, HttpServer}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock

abstract class ServerApp[T <: EJPMType: Tag] extends CliApp[T] with HttpServer with Scheduler {

  final private def createSemaphores(jobs: List[EtlJob]): Task[Map[String, Semaphore]] =
    for {
      rt <- Task.runtime
      semaphores = jobs
        .map(job => (job.name, rt.unsafeRun(Semaphore.make(permits = job.props("job_max_active_runs").toLong))))
        .toMap
    } yield semaphores

  def serverRunner(config: Config): ZIO[ZEnv, Throwable, Unit] = (for {
    _ <- SetTimeZone(config).toManaged_
    authCache = Cache.create[String, String]()
    crypto    = Crypto(config.secretkey)
    listTkn   = config.token.getOrElse(List.empty)
    _         = listTkn.foreach(tkn => authCache.put(tkn, tkn))
    auth      = Authentication(authCache, config.secretkey)
    jobs <- RF.getJobs[T].toManaged_
    sem  <- createSemaphores(jobs).toManaged_
    executor = ServerExecutor[T](sem, config)
    supervisor <- Supervisor.track(true).toManaged_
    dbLayer    = db.liveFullDB(config.db.get, pool_size = 10)
    apiLayer   = Implementation.live[T](auth, executor, jobs, crypto)
    finalLayer = apiLayer ++ dbLayer
    scheduler  = etlFlowScheduler(jobs).supervised(supervisor)
    webserver  = etlFlowWebServer(auth, config.webserver)
    _ <- scheduler.zipPar(webserver).provideSomeLayer[JsonEnv with Blocking with Clock](finalLayer).toManaged_
  } yield ()).useNow.provideCustomLayer(json.Implementation.live)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    Configuration.config.flatMap(cfg => cliRunner(args, cfg, serverRunner(cfg))).exitCode
}
