package etlflow.coretests.utils

import etlflow.coretests.Schema._
import etlflow.utils.{Executor, JsonCirce, LoggingLevel}
import io.circe.generic.auto._
import org.scalatest.{FlatSpec, Matchers}

class JsonCirceTestSuite extends FlatSpec with Matchers {

  val excludeKeys = List("job_run_id","job_description","job_properties","job_aggregate_error")

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
                               |"id":"63",
                               |"class": "101"
                               |}""".stripMargin

  val student2Json: String ="""{
                              |"name":"John",
                              |"id":"63"
                              |}""".stripMargin

  val httpBin: HttpBinResponse = JsonCirce.convertToObject[HttpBinResponse](httpBinJson)
  val student1: Student = JsonCirce.convertToObject[Student](student1Json)
  val student2: Student = JsonCirce.convertToObject[Student](student2Json)

  //Input data for all logging level and deploy mode.
  val inputDebugLevel       = EtlJob23Props("data/movies/ratings/*","test","ratings_par")
  val inputInfoLevel        = EtlJob23Props("data/movies/ratings/*","test","ratings_par")
  val inputJobInput         = EtlJob23Props("data/movies/ratings/*","test","ratings_par")
  val inputJobNegativeInput = EtlJob23Props("data/movies/ratings/*","test","ratings_part")

  //Output data for all logging level and deploy mode.
  val outputDebugLevel = Map(
    "ratings_output_table_name" -> "ratings_par",
    "ratings_input_path" -> "data/movies/ratings/*",
    "ratings_output_dataset" -> "test"
  )

  val outputInfoLevel = Map(
    "ratings_output_table_name" -> "ratings_par",
    "ratings_input_path" -> "data/movies/ratings/*",
    "ratings_output_dataset" -> "test"
  )

  val outputJobLevel = Map(
    "ratings_output_table_name" -> "ratings_par",
    "ratings_input_path" -> "data/movies/ratings/*",
    "ratings_output_dataset" -> "test"
  )

  val outputJobLevelNegative = Map(
    "ratings_output_table_name" -> "ratings_par",
    "ratings_input_path" -> "data/movies/ratings/*",
    "ratings_output_dataset" -> "test"
  )


  val expectedserializerNegativeOutput = """{
                                           | "job_enable_db_logging" : true,
                                           |  "job_send_slack_notification" : true,
                                           |  "job_notification_level" : "info",
                                           |  "ratings_input_path" : "data/movies/ratings/*",
                                           |  "ratings_output_dataset" : "test",
                                           |  "ratings_output_table_name" : "ratings_par"
                                           |}""".stripMargin


  val expectedserializerOutput = """{
                                   |  "ratings_input_path" : "data/movies/ratings/*",
                                   |  "ratings_output_dataset" : "test",
                                   |  "ratings_output_table_name" : "ratings_par"
                                   |}""".stripMargin


  val actualOutputDebugLevel       = JsonCirce.convertToJsonByRemovingKeysAsMap(inputDebugLevel,excludeKeys)
  val actualOutputInfoLevel        = JsonCirce.convertToJsonByRemovingKeysAsMap(inputInfoLevel,excludeKeys)
  val actualOutputJobLevel         = JsonCirce.convertToJsonByRemovingKeysAsMap(inputJobInput,excludeKeys)
  val actualOutputJobLevelNegative = JsonCirce.convertToJsonByRemovingKeysAsMap(inputJobNegativeInput,excludeKeys)
  val actualSerializerInput        = JsonCirce.convertToJsonByRemovingKeys(inputDebugLevel,excludeKeys)


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

}