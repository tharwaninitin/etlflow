package etlflow

import etlflow.api.Implementation
import etlflow.scheduler.Scheduler
import etlflow.utils.{CacheHelper, SetTimeZone}
import etlflow.webserver.Http4sServer
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
      jobs            <- getEtlJobs[EJN](etl_job_props_mapping_package).toManaged_
      jobSemaphores   <- createSemaphores(jobs).toManaged_
      dbLayer         = liveDBWithTransactor(config.dbLog)
      apiLayer        = Implementation.live[EJN](cache,jobSemaphores,jobs,queue,config,etl_job_props_mapping_package)
      finalLayer      = apiLayer ++ dbLayer
      scheduler       = etlFlowScheduler(jobs)
      webserver       = etlFlowWebServer[EJN](cache,jobSemaphores,etl_job_props_mapping_package,queue,config)
      _               <- scheduler.zipPar(webserver).provideCustomLayer(finalLayer).toManaged_
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
