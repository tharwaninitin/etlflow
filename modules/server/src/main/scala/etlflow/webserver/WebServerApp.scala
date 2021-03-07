package etlflow.webserver

import cats.effect.Blocker
import etlflow.utils.CacheHelper
import etlflow.utils.EtlFlowHelper.{CronJob, Props}
import etlflow.{EtlFlowApp, EtlJobProps, EtlJobPropsMapping}
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import zio._
import zio.blocking.Blocking
import scala.reflect.runtime.universe.TypeTag

abstract class WebServerApp[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag]
  extends EtlFlowApp[EJN]
    with Http4sServer {

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    val webServerRunner: ZIO[ZEnv, Throwable, Unit] = (for {
      blocker         <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
      transactor      <- createDbTransactorManaged(config.dbLog, platform.executor.asEC, "EtlFlowWebServer-Pool", 10)(blocker)
      cache           = CacheHelper.createCache[String]
      queue           <- Queue.sliding[(String,String,String,String)](20).toManaged_
      _               = config.token.map( _.foreach( tkn => CacheHelper.putKey(cache,tkn,tkn)))
      cronJobs        <- Ref.make(List.empty[CronJob]).toManaged_
      jobs            <- getEtlJobs[EJN](etl_job_props_mapping_package).toManaged_
      jobSemaphores   <- createSemaphores(jobs).toManaged_
      _               <- etlFlowWebServer[EJN](blocker,cache,jobSemaphores,transactor,etl_job_props_mapping_package,config,queue).provideCustomLayer(liveHttp4s[EJN](transactor,cache,cronJobs,jobSemaphores,jobs,queue)).toManaged_

    } yield ()).use_(ZIO.unit)

    val finalRunner = if (args.isEmpty) webServerRunner else cliRunner(args)

    finalRunner.catchAll{err =>
      UIO {
        logger.error(err.getMessage)
        err.getStackTrace.foreach(x => logger.error(x.toString))
      }
    }.exitCode
  }
}
