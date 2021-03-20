package etlflow

import cats.effect.Blocker
import etlflow.scheduler.SchedulerApp
import etlflow.utils.CacheHelper
import etlflow.utils.EtlFlowHelper.{CronJob, Props}
import etlflow.webserver.Http4sServer
import zio._
import zio.blocking.Blocking
import scala.reflect.runtime.universe.TypeTag
import etlflow.etljobs.{EtlJob => CoreEtlJob}

abstract class ServerApp[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag]
  extends SchedulerApp[EJN]
    with Http4sServer {

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    val serverRunner: ZIO[ZEnv, Throwable, Unit] = (for {
      blocker         <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
      transactor      <- createDbTransactorManaged(config.dbLog, platform.executor.asEC, "EtlFlowSchedulerWebServer-Pool", 10)(blocker)
      queue           <- Queue.sliding[(String,String,String,String)](10).toManaged_
      cache           =  CacheHelper.createCache[String]
      _               =  config.token.map( _.foreach( tkn => CacheHelper.putKey(cache,tkn,tkn)))
      cronJobs        <- Ref.make(List.empty[CronJob]).toManaged_
      jobs            <- getEtlJobs[EJN](etl_job_props_mapping_package).toManaged_
      jobSemaphores   <- createSemaphores(jobs).toManaged_
      _               <- etlFlowScheduler(transactor,cronJobs,jobs,jobSemaphores,queue).fork.toManaged_
      _               <- etlFlowWebServer[EJN](blocker,cache,jobSemaphores,transactor,etl_job_props_mapping_package,config,queue).provideCustomLayer(liveHttp4s[EJN](transactor,cache,cronJobs,jobSemaphores,jobs,queue)).toManaged_
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
