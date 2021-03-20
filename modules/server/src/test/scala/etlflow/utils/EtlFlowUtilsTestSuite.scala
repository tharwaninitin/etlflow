package etlflow.utils

import etlflow.EtlJobProps
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.etljobs.EtlJob
import etlflow.utils.{UtilityFunctions => UF}
import org.scalatest.{FlatSpec, Matchers}

class EtlFlowUtilsTestSuite extends FlatSpec with Matchers with EtlFlowUtils {

  val etl_job_props_mapping_package: String = UF.getJobNamePackage[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]] + "$"

  val props_map_job1: Map[String, String] = getJobActualPropsAsMap[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]("Job1",etl_job_props_mapping_package)
  val props_map_job2: Map[String, String] = getJobActualPropsAsMap[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]("Job2",etl_job_props_mapping_package)

  "Properties Map Job1" should " not returns the Error key in case of success" in {
    assert(!props_map_job1.keySet.contains("error"))
  }

  "Properties Map Job2" should " returns the Error key in map in case of error" in {
    assert(props_map_job2.keySet.contains("error"))
  }
}
