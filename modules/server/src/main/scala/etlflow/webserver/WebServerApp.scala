package etlflow.webserver

import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.utils.{CacheHelper, EtlFlowUtils, SetTimeZone}
import etlflow.utils.EtlFlowHelper.CronJob
import etlflow.webserver.api.ApiImplementation
import etlflow.{EtlFlowApp, EtlJobProps, EtlJobPropsMapping}
import etlflow.jdbc.liveDBWithTransactor
import zio._
import scala.reflect.runtime.universe.TypeTag

abstract class WebServerApp[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag]
  extends EtlFlowApp[EJN] with Http4sServer with EtlFlowUtils {

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    val webServerRunner: ZIO[ZEnv, Throwable, Unit] = (for {
      _               <- SetTimeZone(config).toManaged_
      queue           <- Queue.sliding[(String,String,String,String)](50).toManaged_
      cache           = CacheHelper.createCache[String]
      _               = config.token.map( _.foreach( tkn => CacheHelper.putKey(cache,tkn,tkn)))
      cronJobs        <- Ref.make(List.empty[CronJob]).toManaged_
      jobs            <- getEtlJobs[EJN](etl_job_props_mapping_package).toManaged_
      jobSemaphores   <- createSemaphores(jobs).toManaged_
      dbLayer         = liveDBWithTransactor(config.dbLog)
      apiLayer        = ApiImplementation.live[EJN](cache,cronJobs,jobSemaphores,jobs,queue,config)
      _               <- etlFlowWebServer[EJN](cache,jobSemaphores,etl_job_props_mapping_package,queue,config).provideCustomLayer(apiLayer ++ dbLayer).toManaged_

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
