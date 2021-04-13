package etlflow

import cats.effect.Blocker
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.scheduler.Scheduler
import etlflow.utils.EtlFlowHelper.CronJob
import etlflow.utils.{CacheHelper, SetTimeZone}
import etlflow.webserver.Http4sServer
import etlflow.webserver.api.ApiImplementation
import zio._
import zio.blocking.Blocking
import scala.reflect.runtime.universe.TypeTag

abstract class ServerApp[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag]
  extends EtlFlowApp[EJN] with Http4sServer with Scheduler  {

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    val serverRunner: ZIO[ZEnv, Throwable, Unit] = (for {
      _               <- SetTimeZone(config).toManaged_
      blocker         <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
      transactor      <- createDbTransactorManaged(config.dbLog, platform.executor.asEC, "EtlFlowSchedulerWebServer-Pool", 10)(blocker)
      queue           <- Queue.sliding[(String,String,String,String)](10).toManaged_
      cache           =  CacheHelper.createCache[String]
      _               =  config.token.map( _.foreach( tkn => CacheHelper.putKey(cache,tkn,tkn)))
      cronJobs        <- Ref.make(List.empty[CronJob]).toManaged_
      jobs            <- getEtlJobs[EJN](etl_job_props_mapping_package).toManaged_
      jobSemaphores   <- createSemaphores(jobs).toManaged_
      apiLayer        = ApiImplementation.liveHttp4s[EJN](transactor,cache,cronJobs,jobSemaphores,jobs,queue,config)
      _               <- etlFlowScheduler(transactor,cronJobs,jobs,jobSemaphores,queue).provideCustomLayer(apiLayer).fork.toManaged_
      _               <- etlFlowWebServer[EJN](blocker,cache,jobSemaphores,transactor,etl_job_props_mapping_package,queue,config).provideCustomLayer(apiLayer).toManaged_
    } yield ()).use_(ZIO.unit)

    val finalRunner = if (args.isEmpty) serverRunner else cliRunner(args)

    finalRunner.catchAll{err =>
      UIO {
        logger.error(err.getMessage)
        err.getStackTrace.foreach(x => logger.error(x.toString))
      }
    }.exitCode
  }
}
