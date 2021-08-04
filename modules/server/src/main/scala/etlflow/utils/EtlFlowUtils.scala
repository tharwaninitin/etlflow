package etlflow.utils

import etlflow.EJPMType
import etlflow.cache.CacheApi
import etlflow.db.EtlJob
import etlflow.json.JsonEnv
import etlflow.utils.{ReflectAPI => RF}
import scalacache.caffeine.CaffeineCache
import scalacache.memoization.memoizeSync
import scalacache.modes.sync._
import zio.Runtime.default.unsafeRun
import zio.{RIO, Task, UIO, ZIO}

import scala.reflect.runtime.universe.TypeTag

private [etlflow] trait EtlFlowUtils extends ApplicationLogger{

  implicit val jobPropsMappingCache: CaffeineCache[RIO[JsonEnv,Map[String, String]]] =
    unsafeRun(CacheApi.createCache[RIO[JsonEnv,Map[String, String]]].provideCustomLayer(etlflow.cache.Implementation.live))

  final def getJobPropsMapping[EJN <: EJPMType : TypeTag](job_name: String, ejpm_package: String): RIO[JsonEnv,Map[String, String]] =
    memoizeSync[RIO[JsonEnv,Map[String, String]]](None) {
      Task(RF.getEtlJobPropsMapping[EJN](job_name, ejpm_package)).map{props_mapping =>
        props_mapping.getProps.map(x => (x._1,x._2.toString))
      }
    }

  final def getEtlJobs[EJN <: EJPMType : TypeTag](ejpm_package: String): RIO[JsonEnv,List[EtlJob]] = {
    val jobs = for {
      jobs     <- Task(RF.getEtlJobs[EJN])
      etljobs  <- ZIO.foreach(jobs)(job => getJobPropsMapping[EJN](job,ejpm_package).map(kv => EtlJob(job,kv)))
    } yield etljobs.toList
    jobs.tapError{ e =>
      UIO(logger.error(e.getMessage))
    }
  }
}
