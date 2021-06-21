package etlflow.utils

import etlflow.EJPMType
import etlflow.jdbc.EtlJob
import etlflow.utils.JsonJackson._
import etlflow.utils.{UtilityFunctions => UF}
import scalacache.memoization.memoizeSync
import scalacache.modes.sync._
import zio.{Task, UIO}

import scala.reflect.runtime.universe.TypeTag

private [etlflow] trait EtlFlowUtils {

  implicit val jobPropsMappingCache = CacheHelper.createCache[Map[String, String]]

  final def getJobPropsMapping[EJN <: EJPMType : TypeTag](job_name: String, ejpm_package: String): Map[String, String] = memoizeSync[Map[String, String]](None){
    val props_mapping = UF.getEtlJobName[EJN](job_name, ejpm_package)
    convertToJsonByRemovingKeysAsMap(props_mapping.getProps,List.empty).map(x => (x._1, x._2.toString))
  }

  final def getEtlJobs[EJN <: EJPMType : TypeTag](ejpm_package: String): Task[List[EtlJob]] = {
    Task {
      UF.getEtlJobs[EJN].map(x => EtlJob(x, getJobPropsMapping[EJN](x,ejpm_package))).toList
    }.tapError{ e =>
      UIO(logger.error(e.getMessage))
    }
  }
}
