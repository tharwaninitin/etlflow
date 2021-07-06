package json

import etlflow.json.{Implementation, JsonApi, JsonImplicits}
import etlflow.schema.LoggingLevel
import io.circe.generic.semiauto.deriveEncoder
import json.Schema.Student
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM, environment}

import io.circe.Decoder
import io.circe.Encoder
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import scala.language.implicitConversions
import etlflow.schema.Credential.{JDBC, AWS}

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

  given caseEncoder:Encoder[EtlJob23Props]  = deriveEncoder[EtlJob23Props]

  val inputDebugLevel  = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,true,LoggingLevel.DEBUG)
  val inputInfoLevel   = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,true,LoggingLevel.INFO)
  val inputJobInput         = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,true,LoggingLevel.JOB)
  val inputJobNegativeInput = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,true,LoggingLevel.JOB)

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

  val outputDebugLevel = Map("ratings_output_table_name" -> "ratings_par",
      "ratings_input_path" -> "data/movies/ratings/*",
      "ratings_output_dataset" -> "test",
      "job_send_slack_notification" -> true,
      "job_enable_db_logging" -> true,
      "job_notification_level" -> "debug"
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

  val expectedserializerOutput = """{"ratings_input_path":"data/movies/ratings/*","ratings_output_dataset":"test","ratings_output_table_name":"ratings_par","job_send_slack_notification":true,"job_enable_db_logging":true,"job_notification_level":"debug"}""".stripMargin

  val expected_json = """{"job_max_active_runs":"10","job_name":"etlflow.coretests.jobs.Job3DBSteps","job_description":"","job_props_name":"etlflow.coretests.Schema$EtlJob4Props","job_deploy_mode":"dataproc","job_retry_delay_in_minutes":"0","job_schedule":"0 30 7 ? * *","job_retries":"0","job_send_slack_notification":"false","job_enable_db_logging":"true","job_notification_level":"info"}"""

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
      testM("Circe Json Deserializer : convertToMap : debug mode ") {
        val actualOutputDebugLevel = for {
          actualOutputDebugLevel <- JsonApi.convertToMap(inputDebugLevel,List.empty)
        }  yield actualOutputDebugLevel
        assertM(actualOutputDebugLevel)(equalTo(outputDebugLevel))
      },
      testM("Circe Json Deserializer : convertToMap : info mode") {
        val actualOutputInfoLevel = for {
          actualOutputInfoLevel <- JsonApi.convertToMap(inputInfoLevel,List.empty)
        }  yield actualOutputInfoLevel
        assertM(actualOutputInfoLevel)(equalTo(outputInfoLevel))
      },
      testM("Circe Json Deserializer : convertToMap : job mode") {
        val actualOutputJobLevel = for {
          actualOutputJobLevel <- JsonApi.convertToMap(inputJobInput,List.empty)
        }  yield actualOutputJobLevel
        assertM(actualOutputJobLevel)(equalTo(outputJobLevel))
      },
      testM("Circe Json Serializer   : convertToString : Case class to String") {
        val actualSerializerInput = for {
          actualSerializerInput <- JsonApi.convertToString(inputDebugLevel,List.empty)
        }  yield actualSerializerInput
        assertM(actualSerializerInput)(equalTo(expectedserializerOutput))
      },
      testM("Circe Json Serializer   : convertToString : Map to String") {
        val actualSerializerInput = for {
          actualSerializerInput <- JsonApi.convertToString[Map[String,String]](expected_props_map_job3,List.empty)
          _ = println("actualSerializerInput :" + actualSerializerInput)
        }  yield actualSerializerInput
        assertM(actualSerializerInput)(equalTo(expected_json))
      }
    ).provideLayer(Implementation.live)
}
