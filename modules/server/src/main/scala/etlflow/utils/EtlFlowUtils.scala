package etlflow.utils

import etlflow.EJPMType
import etlflow.db.EtlJob
import etlflow.json.{Implementation, JsonApi}
import etlflow.utils.MailClientApi.logger
import etlflow.utils.{UtilityFunctions => UF}
import scalacache.memoization.memoizeSync
import scalacache.modes.sync._
import zio.{Task, UIO}
import zio.Runtime.default.unsafeRun

import scala.reflect.runtime.universe.TypeTag

private [etlflow] trait EtlFlowUtils {

  implicit val jobPropsMappingCache = CacheHelper.createCache[Task[Map[String, String]]]

  final def getJobPropsMapping[EJN <: EJPMType : TypeTag](job_name: String, ejpm_package: String): Task[Map[String, String]] =
    memoizeSync[Task[Map[String, String]]](None) {
      val props_mapping = UF.getEtlJobName[EJN](job_name, ejpm_package)
      for {
        json <- JsonApi.convertToJsonJacksonByRemovingKeysAsMap(props_mapping.getProps, List.empty).provideLayer(Implementation.live)
        jsonMap = json.map(x => (x._1, x._2.toString))
      } yield jsonMap
    }

  final def getEtlJobs[EJN <: EJPMType : TypeTag](ejpm_package: String): Task[List[EtlJob]] = {
    Task {
      UF.getEtlJobs[EJN].map(x => {
        EtlJob(x, unsafeRun(getJobPropsMapping[EJN](x,ejpm_package)))
      }).toList
    }.tapError{ e =>
      UIO(logger.error(e.getMessage))
    }
  }
}
