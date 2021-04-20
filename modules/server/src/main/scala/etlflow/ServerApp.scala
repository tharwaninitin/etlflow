package etlflow

import etlflow.api.Implementation
import etlflow.executor.Executor
import etlflow.scheduler.Scheduler
import etlflow.utils.{CacheHelper, SetTimeZone}
import etlflow.webserver.{Authentication, Http4sServer}
import etlflow.jdbc.liveDBWithTransactor
import zio._
import scala.reflect.runtime.universe.TypeTag

abstract class ServerApp[EJN <: EJPMType : TypeTag]
  extends EtlFlowApp[EJN] with Http4sServer with Scheduler  {

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    val serverRunner: ZIO[ZEnv, Throwable, Unit] = (for {
      _           <- SetTimeZone(config).toManaged_
      queue       <- Queue.sliding[(String,String,String,String)](10).toManaged_
      cache       = CacheHelper.createCache[String]
      _           = config.token.map( _.foreach( tkn => CacheHelper.putKey(cache,tkn,tkn)))
      auth        = Authentication(authEnabled = true, cache, config.webserver)
      jobs        <- getEtlJobs[EJN](etl_job_props_mapping_package).toManaged_
      sem         <- createSemaphores(jobs).toManaged_
      executor    = Executor[EJN](sem, config, etl_job_props_mapping_package, queue)
      dbLayer     = liveDBWithTransactor(config.dbLog)
      apiLayer    = Implementation.live[EJN](auth,executor,jobs,etl_job_props_mapping_package)
      finalLayer  = apiLayer ++ dbLayer
      scheduler   = etlFlowScheduler(jobs)
      webserver   = etlFlowWebServer[EJN](auth,config.webserver)
      _           <- scheduler.zipPar(webserver).provideCustomLayer(finalLayer).toManaged_
    } yield ()).use_(ZIO.unit)

    cliRunner(args,serverRunner).catchAll{err =>
      UIO {
        logger.error(err.getMessage)
        err.getStackTrace.foreach(x => logger.error(x.toString))
      }
    }.exitCode
  }
}
