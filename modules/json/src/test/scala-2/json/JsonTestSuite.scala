package json

import etlflow.json.{Implementation, JsonApi}
import etlflow.schema.{Executor, LoggingLevel}
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import json.Schema.Student
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM, environment}
import scala.collection.immutable.ListMap

object JsonTestSuite  extends DefaultRunnableSpec{

  implicit val encodeLoggingLevel: Encoder[LoggingLevel] = Encoder[String].contramap {
    case LoggingLevel.INFO => "info"
    case LoggingLevel.JOB => "job"
    case LoggingLevel.DEBUG => "debug"
  }

  implicit val encodeExecutor: Encoder[Executor] = Encoder[String].contramap {
    case Executor.DATAPROC(_, _, _, _, _) => "dataproc"
    case Executor.LOCAL => "local"
    case Executor.LIVY(_) => "livy"
    case Executor.KUBERNETES(_, _, _, _, _, _) => "kubernetes"
    case Executor.LOCAL_SUBPROCESS(_, _, _) => "local-subprocess"
  }

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

  val expected_props_map_job3 = ListMap(Map(
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
  ).toSeq.sortBy(_._2):_*)

  val outputDebugLevel = Map("job_send_slack_notification" -> "true",
      "job_enable_db_logging" -> "true",
      "job_notification_level" -> "debug",
      "ratings_output_table_name" -> "ratings_par",
      "ratings_input_path" -> "data/movies/ratings/*",
      "ratings_output_dataset" -> "test"
    )

  val outputInfoLevel = Map(
      "job_send_slack_notification" -> "true",
      "job_enable_db_logging" -> "true",
      "job_notification_level" -> "info",
      "ratings_output_table_name" -> "ratings_par",
      "ratings_input_path" -> "data/movies/ratings/*",
      "ratings_output_dataset" -> "test"
    )

  val outputJobLevel = Map("job_send_slack_notification" -> "true",
      "job_enable_db_logging" -> "true",
      "job_notification_level" -> "job",
      "ratings_output_table_name" -> "ratings_par",
      "ratings_input_path" -> "data/movies/ratings/*",
      "ratings_output_dataset" -> "test"
    )

  val outputJobLevelNegative = Map("job_send_slack_notification" -> "true",
      "job_enable_db_logging" -> "true",
      "job_notification_level" -> "info",
      "ratings_output_table_name" -> "ratings_par",
      "ratings_input_path" -> "data/movies/ratings/*",
      "ratings_output_dataset" -> "test"
    )

  val expectedserialzerOutput = """{"ratings_input_path":"data/movies/ratings/*","ratings_output_dataset":"test","ratings_output_table_name":"ratings_par","job_send_slack_notification":true,"job_enable_db_logging":true,"job_notification_level":"debug"}""".stripMargin

  val expected_json = """{"job_description":"","job_retry_delay_in_minutes":"0","job_retries":"0","job_schedule":"0 30 7 ? * *","job_max_active_runs":"10","job_deploy_mode":"dataproc","job_props_name":"etlflow.coretests.Schema$EtlJob4Props","job_name":"etlflow.coretests.jobs.Job3DBSteps","job_send_slack_notification":"false","job_notification_level":"info","job_enable_db_logging":"true"}""".stripMargin
  val expected_json1 = """{"job_description":"","job_retry_delay_in_minutes":"0","job_schedule":"0 30 7 ? * *","job_max_active_runs":"10","job_deploy_mode":"dataproc","job_props_name":"etlflow.coretests.Schema$EtlJob4Props","job_name":"etlflow.coretests.jobs.Job3DBSteps","job_send_slack_notification":"false","job_notification_level":"info","job_enable_db_logging":"true"}""".stripMargin

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
        assertM(actualSerializerInput)(equalTo(expectedserialzerOutput))
      },
      testM("Circe Json Serializer   : convertToString : Map to String") {
        val actualSerializerInput = for {
          actualSerializerInput <- JsonApi.convertToString[Map[String,String]](expected_props_map_job3,List.empty)
        }  yield actualSerializerInput
        assertM(actualSerializerInput)(equalTo(expected_json))
      },
      testM("Circe Json Serializer   : convertToString : Map to String with exclude keys") {
        val actualSerializerInput = for {
          actualSerializerInput <- JsonApi.convertToString[Map[String,String]](expected_props_map_job3,List("job_retries"))
          _ = println("actualSerializerInput :" + actualSerializerInput)
        }  yield actualSerializerInput
        assertM(actualSerializerInput)(equalTo(expected_json1))
      }
    ).provideLayer(Implementation.live)
}
