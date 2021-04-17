package etlflow.utils

import caliban.CalibanError.ExecutionError
import etlflow.log.ApplicationLogger
import etlflow.api.Schema._
import etlflow.utils.JsonJackson._
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.EJPMType
import scalacache.memoization.memoizeSync
import scalacache.modes.sync._
import zio.{Semaphore, Task}

import scala.reflect.runtime.universe.TypeTag

trait EtlFlowUtils extends ApplicationLogger {

  implicit val jobPropsCache = CacheHelper.createCache[Map[String, String]]

  final def getPropsCacheStats: CacheDetails = {
    val data:Map[String,String] = CacheHelper.toMap(jobPropsCache)
    val cacheInfo = CacheInfo("JobProps",
      jobPropsCache.underlying.stats.hitCount(),
      jobPropsCache.underlying.stats.hitRate(),
      jobPropsCache.underlying.asMap().size(),
      jobPropsCache.underlying.stats.missCount(),
      jobPropsCache.underlying.stats.missRate(),
      jobPropsCache.underlying.stats.requestCount(),
      data
    )
    CacheDetails("JobProps",JsonJackson.convertToJsonByRemovingKeysAsMap(cacheInfo,List("data")).mapValues(x => (x.toString)))
  }

  final def getEtlJobs[EJN <: EJPMType : TypeTag](etl_job_name_package: String): Task[List[EtlJob]] = {
    Task {
      UF.getEtlJobs[EJN].map(x => EtlJob(x, getJobPropsMapping[EJN](x,etl_job_name_package))).toList
    }.mapError{ e =>
      logger.error(e.getMessage)
      ExecutionError(e.getMessage)
    }
  }

  final def getJobPropsMapping[EJN <: EJPMType : TypeTag](jobName: String, etl_job_name_package: String): Map[String, String] = memoizeSync[Map[String, String]](None){
    val props_mapping = UF.getEtlJobName[EJN](jobName,etl_job_name_package)
    convertToJsonByRemovingKeysAsMap(props_mapping.getProps,List.empty).map(x => (x._1, x._2.toString))
  }

  final def createSemaphores(jobs: List[EtlJob]): Task[Map[String, Semaphore]] = {
    for {
      rt          <- Task.runtime
      semaphores  = jobs.map(job => (job.name, rt.unsafeRun(Semaphore.make(permits = job.props("job_max_active_runs").toLong)))).toMap
    } yield semaphores
  }
}
