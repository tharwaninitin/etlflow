package etlflow.utils

import etlflow.EJPMType
import etlflow.db.EtlJob
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.utils.MailClientApi.logger
import etlflow.utils.{UtilityFunctions => UF}
import scalacache.caffeine.CaffeineCache
import scalacache.memoization.memoizeSync
import scalacache.modes.sync._
import zio.{RIO, Task, UIO, ZIO}
import scala.reflect.runtime.universe.TypeTag

private [etlflow] trait EtlFlowUtils {

  implicit val jobPropsMappingCache: CaffeineCache[RIO[JsonEnv,Map[String, String]]] = CacheHelper.createCache[RIO[JsonEnv,Map[String, String]]]

  final def getJobPropsMapping[EJN <: EJPMType : TypeTag](job_name: String, ejpm_package: String): RIO[JsonEnv,Map[String, String]] =
    memoizeSync[RIO[JsonEnv,Map[String, String]]](None) {
      Task(UF.getEtlJobName[EJN](job_name, ejpm_package)).flatMap{props_mapping =>
        JsonApi.convertToJsonJacksonByRemovingKeysAsMap(props_mapping.getProps, List.empty)
          .map(x => x.mapValues(value => value.toString))
      }
    }

  final def getEtlJobs[EJN <: EJPMType : TypeTag](ejpm_package: String): RIO[JsonEnv,List[EtlJob]] = {
    val jobs = for {
      jobs     <- Task(UF.getEtlJobs[EJN])
      etljobs  <- ZIO.foreach(jobs)(job => getJobPropsMapping[EJN](job,ejpm_package).map(kv => EtlJob(job,kv)))
    } yield etljobs.toList
    jobs.tapError{ e =>
      UIO(logger.error(e.getMessage))
    }
  }
}
