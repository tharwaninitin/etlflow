package etlflow.utils

import etlflow.EtlJobProps
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.etljobs.EtlJob
import etlflow.utils.{UtilityFunctions => UF}
import org.scalatest.{FlatSpec, Matchers}

class EtlflowUtilsTestSuite extends FlatSpec with Matchers  with  EtlFlowUtils {

  val etl_job_props_mapping_package: String = UF.getJobNamePackage[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]] + "$"
  val props_map_job1 = getJobActualProps[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]("Job1",etl_job_props_mapping_package)
  val props_map_job2 = getJobActualProps[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]("Job2",etl_job_props_mapping_package)

  "Properties Map" should " returns the job_deploy_mode as NA in case of error" in {
    assert(props_map_job1.get("job_deploy_mode").get == "NA")
  }

  "Properties Map" should " returns the job_deploy_mode as expected in case of success" in {
    assert(props_map_job2.get("job_deploy_mode").get == "local")
  }

  "Properties Map" should " returns the Error key in map  in case of error" in {
    assert(props_map_job1.keySet.exists(_ == "Error") == true)
  }

  "Properties Map" should " not returns the Error key  in case of success" in {
    assert(props_map_job2.keySet.exists(_ == "Error") == false)
  }
}
