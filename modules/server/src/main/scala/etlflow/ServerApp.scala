package etlflow

import etlflow.scheduler.Scheduler
import etlflow.utils.EtlFlowHelper.CronJob
import etlflow.utils.{CacheHelper, SetTimeZone}
import etlflow.webserver.Http4sServer
import etlflow.webserver.api.ApiImplementation
import etlflow.jdbc.liveDBWithTransactor
import zio._
import scala.reflect.runtime.universe.TypeTag

abstract class ServerApp[EJN <: EJPMType : TypeTag]
  extends EtlFlowApp[EJN] with Http4sServer with Scheduler  {

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    val serverRunner: ZIO[ZEnv, Throwable, Unit] = (for {
      _               <- SetTimeZone(config).toManaged_
      queue           <- Queue.sliding[(String,String,String,String)](10).toManaged_
      cache           =  CacheHelper.createCache[String]
      _               =  config.token.map( _.foreach( tkn => CacheHelper.putKey(cache,tkn,tkn)))
      cronJobs        <- Ref.make(List.empty[CronJob]).toManaged_
      jobs            <- getEtlJobs[EJN](etl_job_props_mapping_package).toManaged_
      jobSemaphores   <- createSemaphores(jobs).toManaged_
      dbLayer         = liveDBWithTransactor(config.dbLog)
      apiLayer        = ApiImplementation.live[EJN](cache,cronJobs,jobSemaphores,jobs,queue,config)
      finalLayer      = apiLayer ++ dbLayer
      schedulerFiber  <- etlFlowScheduler(cronJobs,jobs).provideCustomLayer(finalLayer).fork.toManaged_
      webserverFiber  <- etlFlowWebServer[EJN](cache,jobSemaphores,etl_job_props_mapping_package,queue,config).provideCustomLayer(finalLayer).fork.toManaged_
      _               <- schedulerFiber.zip(webserverFiber).join.toManaged_
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
