package etlflow.webserver

import cats.effect.Blocker
import etlflow.utils.CacheHelper
import etlflow.utils.EtlFlowHelper.CronJob
import etlflow.{EtlFlowApp, EtlJobName, EtlJobProps}
import zio._
import zio.blocking.Blocking
import scala.reflect.runtime.universe.TypeTag

abstract class WebServerApp[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag]
  extends EtlFlowApp[EJN,EJP]
    with Http4sServer {

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    val webServerRunner: ZIO[ZEnv, Throwable, Unit] = (for {
      blocker         <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
      transactor      <- createDbTransactorManaged(config.dbLog, platform.executor.asEC, "EtlFlowWebServer-Pool", 10)(blocker)
      cache           = CacheHelper.createCache[String]
      queue           <- Queue.sliding[(String,String)](20).toManaged_
      _               = config.token.map( _.foreach( tkn => CacheHelper.putKey(cache,tkn,tkn)))
      cronJobs        <- Ref.make(List.empty[CronJob]).toManaged_
      jobs            <- getEtlJobs[EJN,EJP](etl_job_name_package).toManaged_
      jobSemaphores   <- createSemaphores(jobs).toManaged_
      _               <- etlFlowWebServer[EJN,EJP](blocker,cache,jobSemaphores,transactor,etl_job_name_package,config,queue).provideCustomLayer(liveHttp4s[EJN,EJP](transactor,cache,cronJobs,jobSemaphores,jobs,queue)).toManaged_

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
