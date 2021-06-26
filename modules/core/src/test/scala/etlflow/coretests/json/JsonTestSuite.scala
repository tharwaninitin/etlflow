package etlflow.coretests.json

import zio.test.{DefaultRunnableSpec, ZSpec, environment}
import etlflow.json.{Implementation, JsonImplicits, JsonApi}
import etlflow.coretests.Schema._
import zio.test.Assertion.equalTo
import zio.test._
import etlflow.utils.LoggingLevel
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.{EtlJobProps, EtlJobPropsMapping}
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.coretests.Schema._
import etlflow.etljobs.EtlJob
import io.circe.generic.auto._

object JsonTestSuite  extends DefaultRunnableSpec  with JsonImplicits{

  val student1Json: String = """{
                               |"name":"John",
                               |"id":"63",
                               |"class": "101"
                               |}""".stripMargin

  val student2Json: String ="""{
                                |"name":"John",
                                |"id":"63"
                                |}""".stripMargin

  case class EtlJob23Props (
       ratings_input_path: String = "",
       ratings_output_dataset: String = "test",
       ratings_output_table_name: String = "ratings",
       job_send_slack_notification:Boolean = true,
       job_enable_db_logging:Boolean = false,
       job_notification_level:LoggingLevel = LoggingLevel.INFO
  )

  implicit val caseDecoder = deriveEncoder[EtlJob23Props]

  val inputDebugLevel  = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,true,LoggingLevel.DEBUG)
  val inputInfoLevel   = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,true,LoggingLevel.INFO)
  val inputJobInput         = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,true,LoggingLevel.JOB)
  val inputJobNegativeInput = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,true,LoggingLevel.JOB)

  val etl_job_props_mapping_package: String = UF.getJobNamePackage[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]] + "$"
  val props_mapping_job1 = UF.getEtlJobName[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]("Job1",etl_job_props_mapping_package)

  val expected_props_map_job3 = Map(
        "job_send_slack_notification" -> "false",
        "job_enable_db_logging" -> "true",
        "job_notification_level" -> "info",
        "job_max_active_runs" -> "10",
        "job_name" -> "etlflow.coretests.jobs.Job3DBSteps",
        "job_description" -> "",
        "job_props_name" -> "etlflow.coretests.Schema$EtlJob4Props",
        "job_deploy_mode" -> "dataproc",
        "job_retry_delay_in_minutes" -> "0",
        "job_schedule" -> "0 30 7 ? * *",
        "job_retries" -> "0"
      )

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

  val expected_json = """{"job_send_slack_notification":"false","job_enable_db_logging":"true","job_notification_level":"info","job_max_active_runs":"10","job_name":"etlflow.coretests.jobs.Job3DBSteps","job_description":"","job_props_name":"etlflow.coretests.Schema$EtlJob4Props","job_deploy_mode":"dataproc","job_retry_delay_in_minutes":"0","job_schedule":"0 30 7 ? * *","job_retries":"0"}"""

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Json Test")(
      testM("Circe Json Deserializer : ConvertToObject  Student1") {
       val student1 = for {
          name <- JsonApi.convertToObject[Student](student1Json)
        }  yield name
        assertM(student1)(equalTo(Student("63","John",Some("101"))))
      },
      testM("Circe Json Deserializer : ConvertToObject  Student1") {
        val student2 = for {
          name <- JsonApi.convertToObject[Student](student2Json)
        }  yield name
        assertM(student2)(equalTo(Student("63","John",None)))
      },
      testM("Circe Json Deserializer : ConvertToJsonByRemovingKeysAsMap debug") {
        val actualOutputDebugLevel = for {
          actualOutputDebugLevel <- JsonApi.convertToJsonByRemovingKeysAsMap(inputDebugLevel,List.empty)
        }  yield actualOutputDebugLevel
        assertM(actualOutputDebugLevel)(equalTo(outputDebugLevel))
      },
      testM("Circe Json Deserializer : ConvertToJsonByRemovingKeysAsMap info") {
        val actualOutputInfoLevel = for {
          actualOutputInfoLevel <- JsonApi.convertToJsonByRemovingKeysAsMap(inputInfoLevel,List.empty)
        }  yield actualOutputInfoLevel
        assertM(actualOutputInfoLevel)(equalTo(outputInfoLevel))
      },
      testM("Circe Json Deserializer : ConvertToJsonByRemovingKeysAsMap job") {
        val actualOutputJobLevel = for {
          actualOutputJobLevel <- JsonApi.convertToJsonByRemovingKeysAsMap(inputJobInput,List.empty)
        }  yield actualOutputJobLevel
        assertM(actualOutputJobLevel)(equalTo(outputJobLevel))
      },
      testM("Circe Json Serializer : ConvertToJsonByRemovingKeys debug,kubenetes") {
        val actualSerializerInput = for {
          actualSerializerInput <- JsonApi.convertToJsonByRemovingKeys(inputDebugLevel,List.empty)
        }  yield actualSerializerInput.toString()
        assertM(actualSerializerInput)(equalTo(expectedserializerOutput))
      },
      testM("Circe Jackson Serializer : ConvertToJson") {
        val actualSerializerInput = for {
          actualSerializerInput <- JsonApi.convertToJson[Map[String,String]](expected_props_map_job3)
        }  yield actualSerializerInput
        println("actualSerializerInput :" + actualSerializerInput)
        assertM(actualSerializerInput)(equalTo(expected_json))
      },
      testM("Json Jackson Deserializer : ConvertToJsonByRemovingKeysAsMap debug") {
        val actualOutputDebugLevel = for {
          actualOutputDebugLevel <- JsonApi.convertToJsonJacksonByRemovingKeysAsMap(inputDebugLevel,List.empty)
        }  yield actualOutputDebugLevel
        assertM(actualOutputDebugLevel)(equalTo(outputDebugLevel))
      },
      testM("Json Jackson Deserializer : ConvertToJsonByRemovingKeysAsMap info") {
        val actualOutputInfoLevel = for {
          actualOutputInfoLevel <- JsonApi.convertToJsonJacksonByRemovingKeysAsMap(inputInfoLevel,List.empty)
        }  yield actualOutputInfoLevel
        assertM(actualOutputInfoLevel)(equalTo(outputInfoLevel))
      },
      testM("Json Jackson Deserializer : ConvertToJsonByRemovingKeysAsMap job") {
        val actualOutputJobLevel = for {
          actualOutputJobLevel <- JsonApi.convertToJsonJacksonByRemovingKeysAsMap(inputJobInput,List.empty)
        }  yield actualOutputJobLevel
        assertM(actualOutputJobLevel)(equalTo(outputJobLevel))
      },
      testM("Json Jackson Serializer : ConvertToJsonByRemovingKeys debug,kubenetes") {
        val actualSerializerInput = for {
          actualSerializerInput <- JsonApi.convertToJsonJacksonByRemovingKeys(inputDebugLevel,List.empty)
        }  yield actualSerializerInput
        assertM(actualSerializerInput)(equalTo(expectedserializerOutput))
      }
    ).provideLayer(Implementation.live)
}
