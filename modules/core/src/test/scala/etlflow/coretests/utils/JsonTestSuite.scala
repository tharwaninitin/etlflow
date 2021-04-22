package etlflow.coretests.utils

import etlflow.EtlJobProps
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.coretests.Schema._
import etlflow.etljobs.EtlJob
import etlflow.utils.{JsonJackson, LoggingLevel}
import etlflow.utils.{UtilityFunctions => UF}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class JsonTestSuite extends AnyFlatSpec with should.Matchers {

  val httpBinJson = """{
    "args": {
      "param1": "value1"
    },
    "headers": {
      "Accept": "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2",
      "Accept-Encoding": "gzip,deflate",
      "Host": "httpbin.org"
    },
    "origin": "XX.XX.XX.XX",
    "url": "https://httpbin.org/get?param1=value1"
  }"""

  val student1Json: String = """{
                               |"name":"John",
                               |"id":63,
                               |"class": "101"
                               |}""".stripMargin

  val student2Json: String ="""{
                              |"name":"John",
                              |"id":63
                              |}""".stripMargin

  val httpBin: HttpBinResponse = JsonJackson.convertToObject[HttpBinResponse](httpBinJson)
  val student1: Student = JsonJackson.convertToObject[Student](student1Json)
  val student2: Student = JsonJackson.convertToObject[Student](student2Json)

  val excludeKeys = List("job_run_id","job_description","job_properties","job_aggregate_error")

  case class EtlJob23Props (
                             ratings_input_path: String = "",
                             ratings_output_dataset: String = "test",
                             ratings_output_table_name: String = "ratings",
                             job_send_slack_notification:Boolean = true,
                             job_enable_db_logging:Boolean = false,
                             job_notification_level:LoggingLevel = LoggingLevel.INFO
                           )


  //Input data for all logging level and deploy mode.
  val inputDebugLevel       = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,true,LoggingLevel.DEBUG)
  val inputInfoLevel        = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,true,LoggingLevel.INFO)
  val inputJobInput         = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,true,LoggingLevel.JOB)
  val inputJobNegativeInput = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,true,LoggingLevel.JOB)

  val etl_job_props_mapping_package: String = UF.getJobNamePackage[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]] + "$"

  //Output data for all logging level and deploy mode.
  val outputDebugLevel = Map("job_send_slack_notification" -> true,
    "job_enable_db_logging" -> true,
    "job_notification_level" -> "debug",
    "ratings_output_table_name" -> "ratings_par",
    "ratings_input_path" -> "data/movies/ratings/*",
    "ratings_output_dataset" -> "test"
  )
  val outputInfoLevel = Map(
    "job_send_slack_notification" -> true,
    "job_enable_db_logging" -> true,
    "job_notification_level" -> "info",
    "ratings_output_table_name" -> "ratings_par",
    "ratings_input_path" -> "data/movies/ratings/*",
    "ratings_output_dataset" -> "test"
  )
  val outputJobLevel = Map("job_send_slack_notification" -> true,
    "job_enable_db_logging" -> true,
    "job_notification_level" -> "job",
    "ratings_output_table_name" -> "ratings_par",
    "ratings_input_path" -> "data/movies/ratings/*",
    "ratings_output_dataset" -> "test"
  )
  val outputJobLevelNegative = Map("job_send_slack_notification" -> true,
    "job_enable_db_logging" -> true,
    "job_notification_level" -> "info",
    "ratings_output_table_name" -> "ratings_par",
    "ratings_input_path" -> "data/movies/ratings/*",
    "ratings_output_dataset" -> "test"
  )


  val expectedserializerOutput = """{
                                   |  "ratings_input_path" : "data/movies/ratings/*",
                                   |  "ratings_output_dataset" : "test",
                                   |  "ratings_output_table_name" : "ratings_par",
                                   |  "job_send_slack_notification" : true,
                                   |  "job_enable_db_logging" : true,
                                   |  "job_notification_level" : "debug"
                                   |}""".stripMargin

  val expectedserializerNegativeOutput = """{
                             "ratings_input_path" : "data/movies/ratings/*",
                                           |  "ratings_output_dataset" : "test",
                                           |  "ratings_output_table_name" : "ratings_par",
                                           |  "job_send_slack_notification" : true,
                                           |  "job_notification_level" : "debug",
                                           |  "job_enable_db_logging" : true,
                                           |}""".stripMargin


  val actualOutputDebugLevel       = JsonJackson.convertToJsonByRemovingKeysAsMap(inputDebugLevel,excludeKeys)
  val actualOutputInfoLevel        = JsonJackson.convertToJsonByRemovingKeysAsMap(inputInfoLevel,excludeKeys)
  val actualOutputJobLevel         = JsonJackson.convertToJsonByRemovingKeysAsMap(inputJobInput,excludeKeys)
  val actualOutputJobLevelNegative = JsonJackson.convertToJsonByRemovingKeysAsMap(inputJobNegativeInput,excludeKeys)

  val actualSerializerInput = JsonJackson.convertToJsonByRemovingKeys(inputDebugLevel,excludeKeys)

  "Json Deserializer : ConvertToObject  Student1 " should "run successfully" in {
    assert(student1 == Student("63","John",Some("101")))
  }

  "Json Deserializer : ConvertToObject  Student2" should "run successfully" in {
    assert(student2 == Student("63","John",None))
  }

  "Json Deserializer : ConvertToJsonByRemovingKeysAsMap debug,kubenetes" should "run successfully" in {
    assert(actualOutputDebugLevel == outputDebugLevel)
  }

  "Json Deserializer : ConvertToJsonByRemovingKeysAsMap info,kubenetes" should "run successfully" in {
    assert(actualOutputInfoLevel == outputInfoLevel)
  }

  "Json Deserializer : ConvertToJsonByRemovingKeysAsMap job,dataproc " should "run successfully" in {
    assert(actualOutputJobLevel == outputJobLevel)
  }

  //Input has job_notification_level=info and output has job. Test case pass on not matching input and output.
  "Json Deserializer : ConvertToJsonByRemovingKeysAsMap job,local" should "run successfully" in {
    assert(actualOutputJobLevelNegative != outputJobLevelNegative)
  }

  "Json Serializer : ConvertToJsonByRemovingKeys debug,kubenetes" should "run successfully" in {
    assert(actualSerializerInput == expectedserializerOutput)
  }

  //Input has job_notification_level=info and output has "job" notification level. Test case pass on not matching input and output.
  "Json Serializer: ConvertToJsonByRemovingKeys debug,kubenetes negative scenario" should "run successfully" in {
    assert(actualSerializerInput != expectedserializerNegativeOutput)
  }

  "Json Serializer: convertToJsonByRemovingKeysAsMap using EtlJobPropsMapping case with local mode" should "run successfully" in {
    val props_mapping_job1 = UF.getEtlJobName[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]("Job1",etl_job_props_mapping_package)
    val props_map_job1: Map[String, String] = JsonJackson.convertToJsonByRemovingKeysAsMap(props_mapping_job1.getProps,List.empty).map(x => (x._1, x._2.toString))
    val expected_props_map_job1 = Map(
      "job_send_slack_notification" -> "false",
      "job_enable_db_logging" -> "true",
      "job_notification_level" -> "info",
      "job_max_active_runs" -> "10",
      "job_name" -> "etlflow.coretests.jobs.Job1HelloWorld",
      "job_description" -> "",
      "job_props_name" -> "etlflow.coretests.Schema$EtlJob1Props",
      "job_deploy_mode" -> "local",
      "job_retry_delay_in_minutes" -> "0",
      "job_schedule" -> "0 */2 * * * ?",
      "job_retries" -> "0"
    )
    assert(props_map_job1 == expected_props_map_job1)
  }

  "Json Serializer: convertToJsonByRemovingKeysAsMap using EtlJobPropsMapping case with kubernetes mode" should "run successfully" in {
    val props_mapping_job2 = UF.getEtlJobName[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]("Job2",etl_job_props_mapping_package)
    val props_map_job2: Map[String, String] = JsonJackson.convertToJsonByRemovingKeysAsMap(props_mapping_job2.getProps,List.empty).map(x => (x._1, x._2.toString))
    val expected_props_map_job2 = Map(
      "job_send_slack_notification" -> "false",
      "job_enable_db_logging" -> "false",
      "job_notification_level" -> "info",
      "job_max_active_runs" -> "1",
      "job_name" -> "etlflow.coretests.jobs.Job2Retry",
      "job_description" -> "",
      "job_props_name" -> "etlflow.coretests.Schema$EtlJob2Props",
      "job_schedule" -> "0 */15 * * * ?",
      "job_deploy_mode" -> "kubernetes",
      "job_retry_delay_in_minutes" -> "0",
      "job_retries" -> "0"
    )
    assert(props_map_job2 == expected_props_map_job2)
  }

  "Json Serializer: convertToJsonByRemovingKeysAsMap using EtlJobPropsMapping case with dataproc mode" should "run successfully" in {
    val props_mapping_job3 = UF.getEtlJobName[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]("Job3",etl_job_props_mapping_package)
    val props_map_job3: Map[String, String] = JsonJackson.convertToJsonByRemovingKeysAsMap(props_mapping_job3.getProps,List.empty).map(x => (x._1, x._2.toString))
    val expected_props_map_job3 = Map(
      "job_send_slack_notification" -> "false",
      "job_enable_db_logging" -> "true",
      "job_notification_level" -> "info",
      "job_max_active_runs" -> "10",
      "job_name" -> "etlflow.coretests.jobs.Job3HttpSmtpSteps",
      "job_description" -> "",
      "job_props_name" -> "etlflow.coretests.Schema$EtlJob3Props",
      "job_deploy_mode" -> "dataproc",
      "job_retry_delay_in_minutes" -> "0",
      "job_schedule" -> "",
      "job_retries" -> "0"
    )
    assert(props_map_job3 == expected_props_map_job3)
  }

}